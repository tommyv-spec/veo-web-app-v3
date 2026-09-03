"""v698A many-to-one — resolve the pairing AGAINST A DATABASE, not just on paper.

WHY THIS FILE EXISTS. Two sibling tests already cover this feature and both
missed a job-stopping bug, for the same reason. check_audio_from_scene_lint.py
tests the markdown. check_audio_from_scene_plumbing.py says outright that it
"walks the links it can reach without a database". The bug lived in a database
query, so nothing touched it.

That was itself a repeat. pairing_resolver.py was created because the rule it
replaced "lived inside a DB loop (main.py:3482) and was therefore never
unit-tested, which is why the constraint stayed invisible". The 2026-09-03
failure was a DB loop at main.py:3620 — same file, ~140 lines away.

Measured on garnissa v4 (job 15333490), the two defects were:

  1. `Clip.clip_role.in_((None, "", "single"))` — in SQL a NULL never matches an
     IN list, and spoken clips carry clip_role = NULL, so the audio source was
     never found and every pairing reported unresolved. The job refused setup.

  2. `Clip.scene_index == vp.audio_from_scene` — scene_index is 0-based
     (image_platform.py:1639) while audio_from_scene is the author's 1-based
     `### Scene N`. With only defect 1 fixed, each cutaway pairs to the NEXT
     sentence: setup succeeds and the WRONG VIDEO renders. That is the case
     this file cares about most, because nothing else would have caught it.

Runs on in-memory SQLite with a local model that mirrors the columns the query
uses. No server, no network.
"""
import os
import sys

from sqlalchemy import Column, Integer, String, create_engine, or_
from sqlalchemy.orm import declarative_base, sessionmaker

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pairing_resolver import PairingError, resolve_audio_sources  # noqa: E402

Base = declarative_base()


class Clip(Base):
    __tablename__ = "clips"
    id = Column(Integer, primary_key=True)
    scene_index = Column(Integer)              # 0-based, as the real column is
    clip_role = Column(String, nullable=True)  # NULL on spoken clips
    dialogue_text = Column(String)
    audio_from_scene = Column(Integer, nullable=True)   # 1-based scene number


def _build_job():
    """The real garnissa v4 shape: 9 spoken reads, 15 cutaways riding under them."""
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    s = sessionmaker(bind=engine)()
    for i in range(9):
        s.add(Clip(id=100 + i, scene_index=i, clip_role=None,
                   dialogue_text=f"sentence {i + 1}"))
    # (build scene, audio_from_scene) exactly as the build declares them
    riders = [(10, 8), (11, 2), (12, 3), (13, 3), (14, 3), (15, 4), (16, 4),
              (17, 4), (18, 5), (19, 5), (20, 7), (21, 7), (22, 7), (23, 9),
              (24, 9)]
    for n, (scene_no, afs) in enumerate(riders):
        s.add(Clip(id=200 + n, scene_index=scene_no - 1, clip_role="visual_pair",
                   dialogue_text="fragment", audio_from_scene=afs))
    s.commit()
    return s


def _speaks(c):
    return c.clip_role in (None, "", "single") and bool((c.dialogue_text or "").strip())


def _resolve_like_main(s):
    """Mirror of main.py's resolution: resolver decides, DB fetches."""
    clips = s.query(Clip).all()
    rows = {}
    for c in clips:
        no = c.scene_index + 1
        if rows.get(no) is not None and not _speaks(c):
            continue
        rows[no] = {
            "scene_index": no,
            "speaker_mode": ("voiceover" if c.clip_role == "visual_pair"
                             else ("on-camera" if _speaks(c) else "silent")),
            "audio_from_scene": c.audio_from_scene,
            "anchor_node_id": None,
        }
    pairing = resolve_audio_sources(list(rows.values()))
    out = {}
    for c in clips:
        if c.clip_role != "visual_pair":
            continue
        decided = (pairing.get(c.scene_index + 1) or {}).get("audio_source_scene")
        if decided is None:
            out[c.id] = None
            continue
        out[c.id] = s.query(Clip).filter(
            Clip.scene_index == decided - 1,
            or_(Clip.clip_role.is_(None), Clip.clip_role.in_(("", "single"))),
            Clip.dialogue_text.isnot(None),
            Clip.dialogue_text != "",
        ).first()
    return out


failures = []
s = _build_job()

# 1. every cutaway resolves — the job-stopper
resolved = _resolve_like_main(s)
unresolved = [cid for cid, hit in resolved.items() if hit is None]
if unresolved:
    failures.append(f"{len(unresolved)} cutaway(s) did not resolve: {unresolved[:5]}")

# 2. every cutaway resolves to the CORRECT sentence — the silent one
for c in s.query(Clip).filter(Clip.clip_role == "visual_pair").all():
    hit = resolved.get(c.id)
    if hit is None:
        continue
    want = f"sentence {c.audio_from_scene}"
    if hit.dialogue_text != want:
        failures.append(
            f"clip {c.id} (build scene {c.scene_index + 1}) declares "
            f"audio_from_scene={c.audio_from_scene} -> resolved to "
            f"{hit.dialogue_text!r}, expected {want!r} (off-by-one)")

# 3. the defective filter must NOT be reintroduced: prove IN-with-None misses NULL
spoken = s.query(Clip).filter(
    Clip.scene_index == 7,
    Clip.clip_role.in_((None, "", "single")),
).first()
if spoken is not None:
    failures.append("SQL changed: `IN (None, ...)` now matches NULL — "
                    "the guard in this test is no longer meaningful")

# 4. a cutaway pointing at a non-speaking scene is refused, not silently dropped
s2 = _build_job()
s2.query(Clip).filter(Clip.id == 107).update(  # build scene 8 -> make it silent
    {"dialogue_text": "", "clip_role": "visual_pair"})
s2.commit()
try:
    _resolve_like_main(s2)
    failures.append("a cutaway riding under a non-speaking scene was accepted")
except PairingError:
    pass

# 5. main.py really does route through the resolver (no second implementation)
main_src = open(os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "main.py"), encoding="utf-8").read()
if "resolve_audio_sources" not in main_src:
    failures.append("main.py no longer imports resolve_audio_sources — the "
                    "pairing rules have drifted back into a private copy")
if "Clip.clip_role.in_((None," in main_src:
    failures.append("main.py still has a `clip_role.in_((None, ...))` filter, "
                    "which never matches a NULL role")

if failures:
    print("FAIL")
    for f in failures:
        print("  -", f)
    sys.exit(1)
print(f"PASS — 15 cutaways resolve to the correct sentence; "
      f"IN-with-NULL still misses NULL; non-speaking source refused; "
      f"main.py routes through pairing_resolver.")
