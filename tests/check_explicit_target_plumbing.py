"""v889 — the AUTHORED `- **target_duration_s:**` bullet must survive the one
boundary it kept falling off: parsed scene dict -> ImageSceneAssignment row.

WHY THIS FILE EXISTS. The parser reads the bullet and prints it as "(explicit,
authoritative)". prepare_batch_for_video then asks the row for
`explicit_target_s` and lets it OUTRANK the v667 anchor diff. Between those two
there was no column and no constructor kwarg, so the answer was always None and
the override never fired. Nothing failed anywhere. Measured on batch ef5ff43b ->
job 15333490: a build declaring 8/4/6/8/8/4/8/4/8 seconds stored 1.1s on every
spoken clip, because nine sentences reuse one image and so shared one anchor
gap. With `cut_mode: auto` the export ignores the number and the finished video
was unaffected; with `cut_mode: timeline` an 8s sentence would have been cut to
1.1s, silently.

Same shape as check_audio_from_scene_plumbing.py: walk every surface the field
has to cross, then prove the round-trip on a real sqlite table.
"""
import inspect
import os
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

import image_platform  # noqa: E402

FIELD = "explicit_target_s"
ip_src = inspect.getsource(image_platform)

failures = []


def check(ok, why):
    if not ok:
        failures.append(why)


# 1. the column exists on the model
check(hasattr(image_platform.ImageSceneAssignment, FIELD),
      "ImageSceneAssignment is missing the column")

# 2. BOTH migration lists carry it, for the assignments table only (Clip
#    already has its own target_duration_s and must not gain a second one)
check(f'("image_scene_assignments", "{FIELD}",' in ip_src,
      "no migration row for image_scene_assignments")
check(f"ALTER TABLE image_scene_assignments ADD COLUMN {FIELD} REAL" in ip_src,
      "sqlite migration missing")
check(f"ALTER TABLE image_scene_assignments ADD COLUMN IF NOT EXISTS {FIELD} REAL"
      in ip_src, "postgres migration missing")
check(f'("clips", "{FIELD}"' not in ip_src,
      "a clips migration was added for this field — Clip already carries "
      "target_duration_s; two columns for one number is the drift this "
      "whole checker family exists to stop")

# 3. to_dict() serialises it — without this prepare cannot read it back
check(f'"{FIELD}": self.{FIELD}' in ip_src,
      "ImageSceneAssignment.to_dict() does not serialise the field")

# 4. the constructor call passes it — THE boundary it fell off
check(f'{FIELD}=s.get("{FIELD}")' in ip_src,
      "the ImageSceneAssignment(...) call does not pass the field — it is "
      "parsed and thrown away (the v889 failure)")

# 5. the parser still emits it onto the scene dict
check(f'"{FIELD}": explicit_target_s' in ip_src,
      "the parser no longer emits the field onto the scene dict")

# 6. prepare still reads it back at the v667 anchor site
prepare_src = inspect.getsource(image_platform.prepare_batch_for_video)
check(f'scene.get("{FIELD}")' in prepare_src,
      "prepare_batch_for_video no longer reads scene.get(\"explicit_target_s\") "
      "— the authored bullet stops outranking the anchor diff")

# 7. the TRANSPORT row carries it. Without this key on the flat rows the
#    v892.12 verifier's "when the row declares one" condition is false on every
#    row and the duration assertion silently never runs.
flat_rows = ip_src.count(f'"{FIELD}": _explicit')
check(flat_rows >= 2,
      f"scenes_metadata_flat carries the field on {flat_rows} of its 2 builders "
      f"— a prepared row without it makes the v892.12 duration check a no-op")

# 8. round-trip on a real sqlite table, not just on an attribute
engine = create_engine("sqlite://")
image_platform.ImageSceneAssignment.__table__.create(engine)
s = sessionmaker(bind=engine)()
s.add(image_platform.ImageSceneAssignment(
    batch_id="b-round-trip", scene_index=3, lines_json="[]",
    duration_s=None, explicit_target_s=8.0))
s.commit()
row = s.query(image_platform.ImageSceneAssignment).first()
check(row.explicit_target_s == 8.0,
      f"the column did not round-trip: stored 8.0, read {row.explicit_target_s!r}")
d = row.to_dict()
check(d.get(FIELD) == 8.0,
      f"to_dict() did not round-trip: expected 8.0, got {d.get(FIELD)!r}")
check(d.get("duration_s") is None,
      "to_dict() confused the authored bullet with the text_card duration")

if failures:
    print("FAIL")
    for f in failures:
        print("  -", f)
    sys.exit(1)
print("PASS — explicit_target_s has a column, both migrations, the constructor "
      "kwarg, a to_dict key and both transport rows; it round-trips through "
      "sqlite as 8.0 and prepare still reads it back.")
