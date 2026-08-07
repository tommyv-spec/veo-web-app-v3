# v916 — the golden must be free of labs.google site data, not just cookies.
#
# Operator, 2026-08-07: "the copy of the golden folder you are doing is not
# correct, because otherwise it would have worked." Correct.
#
# Their manual step is chrome://settings -> labs.google -> Delete data, which
# clears cookies AND localStorage AND IndexedDB AND service workers. v914 only
# cleaned the Cookies DB, so the copied golden still carried the Flow session
# identity. Measured in a v914-built golden:
#   Local Storage/leveldb        -> 29 labs.google matches
#   IndexedDB/https_labs.google_0.indexeddb.leveldb -> present
#   Service Worker/              -> present
#
# Google SSO lives in cookies, so wiping the shared leveldb stores does not log
# the profile out - verified: 24 google.com cookies survive the purge.

import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))
import worker_profile_pull as w  # noqa: E402


def _fake_profile():
    root = tempfile.mkdtemp()
    d = os.path.join(root, "Default")
    for sub in ("Local Storage/leveldb", "Session Storage", "Service Worker/CacheStorage",
                "IndexedDB/https_labs.google_0.indexeddb.leveldb",
                "IndexedDB/https_mail.google.com_0.indexeddb.leveldb"):
        os.makedirs(os.path.join(d, *sub.split("/")), exist_ok=True)
    with open(os.path.join(d, "Local Storage", "leveldb", "000003.log"), "wb") as f:
        f.write(b"...labs.google session junk...")
    return root


class TestV916PurgeLabsSiteData(unittest.TestCase):
    def test_labs_indexeddb_is_removed(self):
        p = _fake_profile()
        w._purge_labs_site_data(p)
        idb = os.path.join(p, "Default", "IndexedDB")
        left = os.listdir(idb) if os.path.isdir(idb) else []
        self.assertFalse(any("labs.google" in e for e in left))
        shutil.rmtree(p, ignore_errors=True)

    def test_shared_leveldb_stores_are_removed(self):
        """Local/Session Storage share one leveldb across origins and cannot be
        filtered per origin, so on a worker profile they go wholesale."""
        p = _fake_profile()
        w._purge_labs_site_data(p)
        for sub in ("Local Storage", "Session Storage", "Service Worker"):
            self.assertFalse(os.path.isdir(os.path.join(p, "Default", sub)), sub)
        shutil.rmtree(p, ignore_errors=True)

    def test_reports_what_it_removed(self):
        p = _fake_profile()
        removed = w._purge_labs_site_data(p)
        self.assertTrue(any("labs.google" in r for r in removed))
        self.assertIn("Local Storage", removed)
        shutil.rmtree(p, ignore_errors=True)

    def test_missing_dirs_are_not_fatal(self):
        p = tempfile.mkdtemp()
        self.assertEqual([], w._purge_labs_site_data(p))
        shutil.rmtree(p, ignore_errors=True)

    def test_builder_calls_the_purge(self):
        with open(os.path.join(os.path.dirname(__file__), "..", "static",
                               "worker_profile_pull.py"), encoding="utf-8") as f:
            src = f.read()
        self.assertIn("_purge_labs_site_data(tmp", src)


if __name__ == "__main__":
    unittest.main()
