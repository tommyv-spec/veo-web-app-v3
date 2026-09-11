#!/usr/bin/env python3
"""The assignment writer, and the `/v/` branch that had never been exercised.

Codex's A-to-Z audit (2026-09-11) found that `/v/` was labelled "proven live"
when only the PERSONA FALLBACK had ever run — because nothing wrote a
`link_assignments` row, the whole `measure_tag` branch was untested outside a
one-off script. These tests close that: they write a row through the real
endpoint and then resolve a link against it.

    python -m pytest code/tests/test_link_assignments.py -q
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
CODE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CODE))

import models                      # noqa: E402
import image_platform              # noqa: E402  — registers image_nodes for create_all


def tag_of(body: str):
    m = re.search(r"amazon\.com/dp/([A-Z0-9]{10})\?tag=([A-Za-z0-9\-]+)", body)
    return (m.group(1), m.group(2)) if m else (None, None)


class LinkAssignmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        models.init_db(f"sqlite:///{os.path.join(tempfile.mkdtemp(), 'la.db')}")
        import main                                  # noqa: E402
        from fastapi.testclient import TestClient
        cls.main, cls.c = main, TestClient(main.app)

    def setUp(self):
        with models.get_db() as db:
            db.query(models.LinkAssignment).delete()
            db.commit()

    def assign(self, post_id, persona="martha", tag="kvm01-20", days=21, asin=None):
        body = {"post_id": post_id, "persona": persona}
        if tag:
            body["measure_tag"] = tag
            body["measure_until"] = (datetime.utcnow() + timedelta(days=days)).isoformat()
        if asin:
            body["asin"] = asin
        return self.c.post("/api/link-assignments", json=body)

    # ---- the branch the audit said was never proven -------------------------
    def test_a_populated_row_makes_the_link_resolve_to_the_measure_tag(self):
        self.assertEqual(200, self.assign("martha-20260911-1").status_code)
        r = self.c.get("/v/martha/martha-20260911-1")
        self.assertEqual(200, r.status_code)
        self.assertEqual(("B0GVKNB82S", "kvm01-20"), tag_of(r.text))

    def test_when_the_window_passes_the_same_link_falls_back_with_no_write(self):
        self.assign("martha-20260911-2")
        with models.get_db() as db:
            row = db.query(models.LinkAssignment).filter_by(
                post_id="martha-20260911-2").first()
            row.measure_until = datetime.utcnow() - timedelta(minutes=1)
            db.commit()
        r = self.c.get("/v/martha/martha-20260911-2")
        self.assertEqual(("B0GVKNB82S", "kavenokorella3-20"), tag_of(r.text))

    def test_the_wrong_persona_does_not_borrow_another_accounts_measure_tag(self):
        self.assign("martha-20260911-3", persona="martha", tag="kvm02-20")
        r = self.c.get("/v/nuri/martha-20260911-3")
        self.assertEqual(("B0GVKNB82S", "kavenokorel00-20"), tag_of(r.text))

    # ---- the writer ---------------------------------------------------------
    def test_upsert_converges_rather_than_failing_on_a_repeat(self):
        first = self.assign("martha-20260911-4")
        second = self.assign("martha-20260911-4", tag="kvm03-20")
        self.assertTrue(first.json()["created"])
        self.assertFalse(second.json()["created"])
        r = self.c.get("/v/martha/martha-20260911-4")
        self.assertEqual("kvm03-20", tag_of(r.text)[1])

    def test_a_malformed_tag_is_refused_at_the_writer(self):
        """The resolver falls back rather than erroring, so a bad row would
        degrade attribution silently. It has to be caught here."""
        r = self.assign("x1", tag="not a tag!!")
        self.assertEqual(400, r.status_code)

    def test_a_malformed_asin_is_refused(self):
        self.assertEqual(400, self.assign("x2", asin="../../etc").status_code)

    def test_an_unknown_persona_is_refused(self):
        self.assertEqual(400, self.assign("x3", persona="nobody").status_code)

    def test_a_tag_with_no_window_is_refused(self):
        r = self.c.post("/api/link-assignments", json={
            "post_id": "x4", "persona": "martha", "measure_tag": "kvm04-20"})
        self.assertEqual(400, r.status_code)
        self.assertIn("measure forever", r.json()["detail"])

    def test_an_over_long_post_id_is_refused(self):
        self.assertEqual(400, self.assign("y" * 65).status_code)

    # ---- release ------------------------------------------------------------
    def test_release_returns_only_expired_ids_and_is_idempotent(self):
        self.assign("martha-20260911-5", tag="kvm05-20")          # still running
        self.assign("martha-20260911-6", tag="kvm06-20")
        with models.get_db() as db:
            row = db.query(models.LinkAssignment).filter_by(
                post_id="martha-20260911-6").first()
            row.measure_until = datetime.utcnow() - timedelta(days=1)
            db.commit()
        first = self.c.post("/api/link-assignments/release").json()
        self.assertEqual(1, first["released"])
        self.assertEqual(["kvm06-20"], first["tags"])
        again = self.c.post("/api/link-assignments/release").json()
        self.assertEqual(0, again["released"], "a second run must find nothing")

    def test_releasing_does_not_change_how_a_link_resolves(self):
        """Release frees the ID. The LINK already fell back at read time."""
        self.assign("martha-20260911-7", tag="kvm07-20")
        with models.get_db() as db:
            row = db.query(models.LinkAssignment).filter_by(
                post_id="martha-20260911-7").first()
            row.measure_until = datetime.utcnow() - timedelta(days=1)
            db.commit()
        before = tag_of(self.c.get("/v/martha/martha-20260911-7").text)
        self.c.post("/api/link-assignments/release")
        after = tag_of(self.c.get("/v/martha/martha-20260911-7").text)
        self.assertEqual(before, after)
        self.assertEqual("kavenokorella3-20", after[1])

    def test_the_listing_reports_which_assignments_are_still_measuring(self):
        self.assign("martha-20260911-8", tag="kvm08-20")
        rows = self.c.get("/api/link-assignments").json()["assignments"]
        row = [r for r in rows if r["post_id"] == "martha-20260911-8"][0]
        self.assertTrue(row["measuring"])
        self.assertIsNone(row["released_at"])


if __name__ == "__main__":
    unittest.main()
