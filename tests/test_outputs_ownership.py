#!/usr/bin/env python3
"""GET /api/jobs/{id}/outputs must only list outputs to the job's owner.

Until 2026-09-13 this route looked the job up with no owner filter and, when
the row was missing, listed outputs_dir/<job_id> straight off disk. Any
signed-in user holding a job id could therefore read another user's output
filenames — the exact names the public download route relies on being
unguessable. The route now uses get_user_job, the same guard every other
/api/jobs/{id} route uses.

Three cases, one test each: a non-owner is refused 403, a job with no row is
404 (the old filesystem fallback is gone), and the owner still gets the list.

    python -m pytest code/tests/test_outputs_ownership.py -q
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
CODE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CODE))

import models                      # noqa: E402
import image_platform              # noqa: E402  — registers image_nodes for create_all

OWNER_ID = "11111111-1111-4111-8111-111111111111"
STRANGER_ID = "22222222-2222-4222-8222-222222222222"
JOB_ID = "33333333-3333-4333-8333-333333333333"


class OutputsOwnershipTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        models.init_db(f"sqlite:///{os.path.join(cls.tmp, 'outputs.db')}")
        import main                                  # noqa: E402
        cls.main = main
        from fastapi import HTTPException
        cls.HTTPException = HTTPException

        cls.out_dir = Path(cls.tmp) / "outputs" / JOB_ID
        cls.out_dir.mkdir(parents=True, exist_ok=True)
        (cls.out_dir / "clip_0.mp4").write_bytes(b"not really a video")

        with models.get_db() as db:
            db.add(models.User(id=OWNER_ID, email="owner@example.com"))
            db.add(models.User(id=STRANGER_ID, email="stranger@example.com"))
            db.add(models.Job(
                id=JOB_ID,
                user_id=OWNER_ID,
                config_json="{}",
                dialogue_json="[]",
                images_dir=str(Path(cls.tmp) / "images"),
                output_dir=str(cls.out_dir),
            ))
            db.commit()

    def call(self, job_id, user_id):
        with models.get_db() as db:
            user = db.query(models.User).filter(models.User.id == user_id).first()
            return asyncio.run(self.main.list_outputs(job_id, db=db, current_user=user))

    def test_a_stranger_is_refused_and_learns_no_filenames(self):
        with self.assertRaises(self.HTTPException) as caught:
            self.call(JOB_ID, STRANGER_ID)
        self.assertEqual(403, caught.exception.status_code)

    def test_a_job_with_no_row_is_404_not_a_directory_listing(self):
        """The old filesystem fallback listed any directory under outputs_dir."""
        with self.assertRaises(self.HTTPException) as caught:
            self.call("44444444-4444-4444-8444-444444444444", STRANGER_ID)
        self.assertEqual(404, caught.exception.status_code)

    def test_the_owner_still_gets_the_list(self):
        out = self.call(JOB_ID, OWNER_ID)
        self.assertEqual(JOB_ID, out["job_id"])
        self.assertEqual(["clip_0.mp4"], [v["filename"] for v in out["videos"]])
        self.assertEqual(1, out["count"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
