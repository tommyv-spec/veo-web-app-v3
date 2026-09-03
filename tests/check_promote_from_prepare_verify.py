"""v892.12 — watch the post-create verifier FAIL on the shapes that matter.

A check nobody has seen fail is not evidence. This file writes real Clip rows
into an in-memory SQLite database (same style as
check_audio_from_scene_db_resolution.py, which exists because two paper tests
missed a bug that lived in a query), reads them back the way the endpoint does,
and requires the verifier to pass on the good shape and to FAIL on five bad
ones:

  (a) every clip_role and audio_from_scene NULL — the 2026-09-03 job exactly.
      Setup succeeded, every gate was green, and Veo was asked to lip-sync a
      sentence fragment onto a shot with no face.
  (b) every cutaway's scene_index shifted +1 — the silent off-by-one: each
      cutaway rides under the NEXT sentence and the video is simply wrong.
  (c) audio_from_scene stored 0-based — a "helpful" -1 by a future editor.
  (d) 23 clips instead of 24 — a dropped silent row (the pre-v682f bug).
  (e) target_duration_s = the anchor gap while the build declared 8s — v889.

It also covers check_prepare_transport, which runs BEFORE any job exists and is
what stops (e) from being unverifiable: the verifier only checks rows that
DECLARE a duration, so a prepared row that lost the key would make that check
skip itself in silence.
"""
import json
import os
import sys

from sqlalchemy import Column, Float, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from promote_from_prepare import (  # noqa: E402
    check_prepare_transport,
    expected_rows_from_prepare,
    verify_promoted_clips,
)

Base = declarative_base()


class Clip(Base):
    """The columns the endpoint reads back — same names as the real table."""
    __tablename__ = "clips"
    id = Column(Integer, primary_key=True)
    clip_index = Column(Integer)
    scene_index = Column(Integer)               # 0-based, as the real column is
    clip_role = Column(String, nullable=True)   # NULL on spoken clips
    dialogue_text = Column(String)
    audio_from_scene = Column(Integer, nullable=True)   # 1-based scene number
    target_duration_s = Column(Float, nullable=True)


with open(os.path.join(HERE, "fixtures", "prepare_response_garnissa_v4.json"),
          encoding="utf-8") as fh:
    PREPARED = json.load(fh)

EXPECTED = expected_rows_from_prepare(PREPARED)
SCENE_POSITION = {sa["scene_index"]: pos
                  for pos, sa in enumerate(PREPARED["scene_assignments"])}

failures = []


def check(ok, why):
    if not ok:
        failures.append(why)


def write_job(mutate=None):
    """Write the reference job's clips, optionally bent out of shape first."""
    rows = []
    for i, row in enumerate(EXPECTED):
        rows.append({
            "id": 1000 + i,
            "clip_index": i,
            "scene_index": SCENE_POSITION[row["scene_index"]],
            "clip_role": row.get("clip_role"),
            "dialogue_text": row.get("dialogue_text"),
            "audio_from_scene": row.get("audio_from_scene"),
            # what a correct write stores: the authored bullet where the build
            # declared one, else nothing
            "target_duration_s": row.get("explicit_target_s"),
        })
    if mutate:
        rows = mutate(rows)
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    s = sessionmaker(bind=engine)()
    for r in rows:
        s.add(Clip(**r))
    s.commit()
    return s


def read_back(s):
    """Exactly what the endpoint hands the verifier: plain dicts, clip order."""
    return [{
        "clip_index": c.clip_index,
        "scene_index": c.scene_index,
        "clip_role": c.clip_role,
        "dialogue_text": c.dialogue_text,
        "audio_from_scene": c.audio_from_scene,
        "target_duration_s": c.target_duration_s,
    } for c in s.query(Clip).order_by(Clip.clip_index).all()]


def verify(mutate=None):
    return verify_promoted_clips(EXPECTED, read_back(write_job(mutate)),
                                 SCENE_POSITION)


# --- positive: the correct write passes -----------------------------------
problems = verify()
check(problems == [],
      f"the correct reference shape must verify clean, got: {problems[:4]}")


def expect_fail(label, mutate, needle):
    got = verify(mutate)
    if not got:
        failures.append(f"{label}: the verifier PASSED a shape it must refuse")
        return
    if not any(needle in p for p in got):
        failures.append(
            f"{label}: refused, but no problem line mentions {needle!r}; "
            f"got {got[:2]}")


# (a) the 2026-09-03 shape — nothing carries a role or a source scene
def _all_null(rows):
    for r in rows:
        r["clip_role"] = None
        r["audio_from_scene"] = None
    return rows


expect_fail("(a) all roles NULL", _all_null, "clip_role histogram")

# (b) every cutaway one scene too far along
def _shift(rows):
    for r in rows:
        if r["clip_role"] == "visual_pair":
            r["scene_index"] += 1
    return rows


expect_fail("(b) scene_index +1", _shift, "0-based POSITION")

# (c) audio_from_scene written 0-based
def _zero_based(rows):
    for r in rows:
        if r["audio_from_scene"] is not None:
            r["audio_from_scene"] -= 1
    return rows


expect_fail("(c) audio_from_scene 0-based", _zero_based,
            "passed through unconverted")

# (d) a dropped row
expect_fail("(d) 23 clips", lambda rows: rows[:-1], "count")

# (e) v889 — the anchor gap stored instead of the authored bullet
def _anchor_gap(rows):
    for r in rows:
        if r["target_duration_s"] is not None:
            r["target_duration_s"] = 1.1
    return rows


expect_fail("(e) authored duration lost", _anchor_gap, "v889")

# a spoken clip must never carry audio_from_scene
def _spoken_rides(rows):
    rows[0]["audio_from_scene"] = 2
    return rows


expect_fail("spoken clip carries audio_from_scene", _spoken_rides,
            "only a visual_pair")

# --- check_prepare_transport ----------------------------------------------
ASSIGN = [{"scene_index": 1, "explicit_target_s": 8.0}]

check(check_prepare_transport(
    ASSIGN, [{"scene_index": 1, "explicit_target_s": 8.0}]) == [],
    "a matching pair must be clean")

missing_key = check_prepare_transport(ASSIGN, [{"scene_index": 1}])
check(len(missing_key) == 1 and "no explicit_target_s key" in missing_key[0],
      f"a prepared row without the key must be one problem, got {missing_key}")

wrong_value = check_prepare_transport(
    ASSIGN, [{"scene_index": 1, "explicit_target_s": 1.1}])
check(len(wrong_value) == 1 and "1.1" in wrong_value[0],
      f"a prepared row with the anchor gap must be one problem, got {wrong_value}")

check(check_prepare_transport([{"scene_index": 1, "explicit_target_s": None}],
                              [{"scene_index": 1}]) == [],
      "a scene that declares nothing must not be reported")

check(check_prepare_transport(PREPARED["scene_assignments"],
                              PREPARED["scenes_metadata"]) == [],
      "the reference prepare response must pass its own transport check")

if failures:
    print("FAIL")
    for f in failures:
        print("  -", f)
    sys.exit(1)
print("PASS — the reference shape verifies clean; the verifier REFUSES "
      "all-NULL roles, a +1 scene shift, 0-based audio_from_scene, a missing "
      "clip, an anchor-gap duration and a spoken clip carrying "
      "audio_from_scene; check_prepare_transport catches a dropped and a "
      "wrong authored duration before any job exists.")
