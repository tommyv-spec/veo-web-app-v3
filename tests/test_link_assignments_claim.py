#!/usr/bin/env python3
"""The claim that has to be atomic: one open holder per tracking id. WP4.

The bug this closes. `tools/tracking_pool.py allocate` read every assignment
over HTTP, worked out which tags were free in the client, picked one, and POSTed
it back. Two allocators running together both read the same free tag and both
wrote it. One id then measured two videos and every sale landed on whichever of
them Amazon saw — with nothing in any report saying so, because the table was
keyed only on `post_id` and a shared `measure_tag` was a perfectly legal row.

So the tests here are mostly about the SECOND caller: what it gets, and what it
must never get. The two-thread case is the one that matters, because the
single-threaded check ("is this tag free?") passes for both callers when they
run on the same millisecond — only the partial unique index decides it.

    python -m pytest code/tests/test_link_assignments_claim.py -q
"""
from __future__ import annotations

import os
import queue
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
CODE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CODE))

import models                      # noqa: E402
import image_platform              # noqa: E402  — registers image_nodes for create_all

POOL = [f"kvpool{n:02d}-20" for n in range(1, 6)]


class ClaimTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        models.init_db(f"sqlite:///{os.path.join(tempfile.mkdtemp(), 'claim.db')}")
        import main                                  # noqa: E402
        from fastapi.testclient import TestClient
        cls.main, cls.c = main, TestClient(main.app)

    def setUp(self):
        with models.get_db() as db:
            db.query(models.LinkAssignment).delete()
            db.commit()
        self.main._ACTIVE_TAG_GUARD = None

    def claim(self, post_id, persona="martha", tags=None, days=21, **extra):
        body = {"post_id": post_id, "persona": persona,
                "candidate_tags": POOL if tags is None else tags,
                "measure_days": days}
        body.update(extra)
        return self.c.post("/api/link-assignments/claim", json=body)

    def expire(self, post_id):
        with models.get_db() as db:
            row = db.query(models.LinkAssignment).filter_by(post_id=post_id).first()
            row.measure_until = datetime.utcnow() - timedelta(minutes=1)
            db.commit()

    # ---- the guarantee is a real index, not a comment -----------------------
    def test_the_database_actually_carries_the_active_tag_index(self):
        """If this fails, every other test here proves only the SELECT."""
        from sqlalchemy import inspect
        names = {i["name"] for i in
                 inspect(models.engine).get_indexes("link_assignments")}
        self.assertIn(models.ACTIVE_TAG_INDEX, names)
        self.assertEqual("unique-index", self.claim("guard-1").json()["guard"])

    def test_the_index_refuses_a_second_open_holder_at_the_database(self):
        """Bypass the endpoint entirely — write two rows straight to the table."""
        from sqlalchemy.exc import IntegrityError
        with models.get_db() as db:
            db.add(models.LinkAssignment(
                post_id="raw-1", asin="B0GVKNB82S", persona="martha",
                measure_tag="kvpool01-20",
                measure_until=datetime.utcnow() + timedelta(days=21)))
            db.commit()
        with self.assertRaises(IntegrityError):
            with models.get_db() as db:
                db.add(models.LinkAssignment(
                    post_id="raw-2", asin="B0GVKNB82S", persona="noemi",
                    measure_tag="kvpool01-20",
                    measure_until=datetime.utcnow() + timedelta(days=21)))
                db.commit()

    # ---- two claimers -------------------------------------------------------
    def test_the_index_and_not_the_check_is_what_decides_the_race(self):
        """Force the interleaving SQLite would otherwise serialize away.

        The plain two-thread test below can pass on a database that takes one
        writer at a time, which would prove nothing about the guarantee. So
        this one puts the two callers in the order that actually breaks the old
        allocator: thread A passes the "is this tag free?" SELECT and stalls
        exactly where a real scheduler could stall it; thread B claims the tag
        and commits; A wakes and writes. A must LOSE — at the index, since its
        own check said yes — and move to the next candidate on its own.
        """
        orig_init = models.LinkAssignment.__init__
        reached, resume, first = threading.Event(), threading.Event(), {"done": False}

        def stalling_init(inner_self, *a, **kw):
            orig_init(inner_self, *a, **kw)
            if not first["done"]:
                first["done"] = True
                reached.set()
                resume.wait(timeout=20)

        out = {}
        models.LinkAssignment.__init__ = stalling_init
        try:
            a = threading.Thread(target=lambda: out.update(
                a=self.claim("stall-a", tags=["kvpool01-20", "kvpool02-20"]).json()))
            a.start()
            self.assertTrue(reached.wait(timeout=20), "A never reached its write")
            b = self.claim("stall-b", tags=["kvpool01-20"])
            self.assertEqual(200, b.status_code, b.text)
            self.assertEqual("kvpool01-20", b.json()["tag"])
            resume.set()
            a.join(timeout=30)
            self.assertFalse(a.is_alive(), "A hung after losing the race")
        finally:
            models.LinkAssignment.__init__ = orig_init
            resume.set()

        self.assertEqual("kvpool02-20", out.get("a", {}).get("tag"),
                         f"A had to give up kvpool01-20 and take the next id: {out}")

    def test_two_concurrent_claimers_never_get_the_same_tag(self):
        """The race the read-then-write allocator lost.

        Both threads see an empty table and both want the first pool tag. One
        of them has to end up on `kvpool02-20`, and neither may fail.
        """
        out = queue.Queue()
        start = threading.Barrier(2)

        def claimer(post_id):
            try:
                start.wait(timeout=10)
                r = self.claim(post_id)
                out.put((post_id, r.status_code, r.json()))
            except Exception as exc:                       # pragma: no cover
                out.put((post_id, -1, {"error": f"{type(exc).__name__}: {exc}"}))

        threads = [threading.Thread(target=claimer, args=(p,))
                   for p in ("race-a", "race-b")]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
            self.assertFalse(t.is_alive(), "a claimer hung — it should never block")

        results = [out.get_nowait() for _ in range(2)]
        for post_id, status, body in results:
            self.assertEqual(200, status, f"{post_id} failed: {body}")
        tags = sorted(b["tag"] for _p, _s, b in results)
        self.assertEqual(2, len(set(tags)), f"both claimers got {tags}")
        self.assertEqual(["kvpool01-20", "kvpool02-20"], tags)

        # And the table agrees — two posts, two ids, both open.
        rows = self.c.get("/api/link-assignments").json()["assignments"]
        mine = {r["post_id"]: r["measure_tag"] for r in rows
                if r["post_id"] in ("race-a", "race-b")}
        self.assertEqual(2, len(set(mine.values())))

    def test_a_second_claim_cannot_take_an_open_tag_even_when_asked_for_it(self):
        first = self.claim("seq-a", tags=["kvpool01-20"])
        self.assertEqual("kvpool01-20", first.json()["tag"])
        second = self.claim("seq-b", tags=["kvpool01-20"])
        self.assertEqual(409, second.status_code)
        self.assertIn("open holder", second.json()["detail"])

    def test_a_second_claim_skips_the_held_tag_and_takes_the_next(self):
        self.claim("skip-a", tags=["kvpool01-20", "kvpool02-20"])
        second = self.claim("skip-b", tags=["kvpool01-20", "kvpool02-20"])
        self.assertEqual(200, second.status_code)
        self.assertEqual("kvpool02-20", second.json()["tag"])

    # ---- release, then re-claim --------------------------------------------
    def test_an_expired_tag_stays_blocked_until_release_clears_the_holder(self):
        """Expiry alone is not release. Fail closed until the id is handed back."""
        self.claim("exp-a", tags=["kvpool01-20"])
        self.expire("exp-a")
        blocked = self.claim("exp-b", tags=["kvpool01-20"])
        self.assertEqual(409, blocked.status_code)

    def test_release_then_reclaim_succeeds(self):
        self.claim("rel-a", tags=["kvpool01-20"])
        self.expire("rel-a")
        released = self.c.post("/api/link-assignments/release").json()
        self.assertEqual(["kvpool01-20"], released["tags"])
        again = self.claim("rel-b", tags=["kvpool01-20"])
        self.assertEqual(200, again.status_code, again.text)
        self.assertEqual("kvpool01-20", again.json()["tag"])

    def test_a_released_tag_does_not_keep_resolving_for_its_old_post(self):
        """The re-claim must not leave two live answers for one id."""
        self.claim("old-post", persona="martha", tags=["kvpool01-20"])
        self.expire("old-post")
        self.c.post("/api/link-assignments/release")
        self.claim("new-post", persona="martha", tags=["kvpool01-20"])
        old = self.c.get("/v/martha/old-post").text
        self.assertIn("kavenokorella3-20", old, "the old post must fall back")
        self.assertIn("kvpool01-20", self.c.get("/v/martha/new-post").text)

    # ---- exhaustion ---------------------------------------------------------
    def test_an_exhausted_pool_is_a_409_and_writes_nothing(self):
        for i, tag in enumerate(POOL):
            self.assertEqual(200, self.claim(f"full-{i}", tags=[tag]).status_code)
        r = self.claim("one-too-many")
        self.assertEqual(409, r.status_code)
        self.assertIn("no tracking id is free", r.json()["detail"])
        rows = self.c.get("/api/link-assignments").json()["assignments"]
        self.assertEqual([], [x for x in rows if x["post_id"] == "one-too-many"])

    def test_an_empty_candidate_list_is_refused_not_silently_ignored(self):
        self.assertEqual(400, self.claim("empty", tags=[]).status_code)
        self.assertEqual(400, self.c.post("/api/link-assignments/claim", json={
            "post_id": "empty2", "persona": "martha"}).status_code)

    # ---- once per post ------------------------------------------------------
    def test_a_repeat_claim_returns_the_same_tag_and_spends_nothing(self):
        first = self.claim("once").json()
        second = self.claim("once").json()
        self.assertEqual(first["tag"], second["tag"])
        self.assertTrue(first["claimed"])
        self.assertFalse(second["claimed"])
        held = [r for r in self.c.get("/api/link-assignments").json()["assignments"]
                if r["measure_tag"]]
        self.assertEqual(1, len(held), "a retry must not consume a second id")

    def test_a_post_whose_id_was_released_is_never_given_another(self):
        """Re-tagging that row would erase the release stamp the cool-off needs."""
        self.claim("spent", tags=["kvpool01-20"])
        self.expire("spent")
        self.c.post("/api/link-assignments/release")
        r = self.claim("spent", tags=["kvpool02-20"])
        self.assertEqual(409, r.status_code)
        self.assertIn("never given a second id", r.json()["detail"])

    def test_a_post_filed_under_another_persona_is_refused(self):
        self.claim("crossed", persona="martha")
        r = self.claim("crossed", persona="nuri")
        self.assertEqual(409, r.status_code)
        self.assertIn("filed under persona", r.json()["detail"])

    # ---- validation ---------------------------------------------------------
    def test_a_malformed_candidate_tag_is_refused_before_anything_is_written(self):
        r = self.claim("bad", tags=["kvpool01-20", "not a tag!!"])
        self.assertEqual(400, r.status_code)
        self.assertEqual([], self.c.get("/api/link-assignments").json()["assignments"])

    def test_an_unknown_persona_is_refused(self):
        self.assertEqual(400, self.claim("who", persona="nobody").status_code)

    def test_a_nonsense_window_is_refused(self):
        self.assertEqual(400, self.claim("w1", days=0).status_code)
        self.assertEqual(400, self.claim("w2", days=4000).status_code)

    def test_the_window_is_the_days_the_caller_asked_for(self):
        body = self.claim("window", days=21).json()
        until = datetime.fromisoformat(body["measure_until"])
        self.assertAlmostEqual(21, (until - datetime.utcnow()).days + 1, delta=1)

    # ---- the claimed tag actually resolves ----------------------------------
    def test_the_claimed_tag_is_what_the_published_link_resolves_to(self):
        body = self.claim("resolves", persona="noemi", tags=["kvpool03-20"]).json()
        self.assertIn(body["tag"], self.c.get("/v/noemi/resolves").text)

    # ---- the old upsert endpoint still works, and now fails loudly ----------
    def test_the_upsert_endpoint_still_writes_a_row(self):
        r = self.c.post("/api/link-assignments", json={
            "post_id": "upsert-1", "persona": "martha",
            "measure_tag": "kvpool04-20",
            "measure_until": (datetime.utcnow() + timedelta(days=21)).isoformat()})
        self.assertEqual(200, r.status_code, r.text)
        self.assertTrue(r.json()["created"])

    def test_the_upsert_endpoint_returns_409_not_500_on_a_held_tag(self):
        self.claim("holder", tags=["kvpool05-20"])
        r = self.c.post("/api/link-assignments", json={
            "post_id": "thief", "persona": "martha",
            "measure_tag": "kvpool05-20",
            "measure_until": (datetime.utcnow() + timedelta(days=21)).isoformat()})
        self.assertEqual(409, r.status_code)
        self.assertIn("holder", r.json()["detail"])


if __name__ == "__main__":
    unittest.main()
