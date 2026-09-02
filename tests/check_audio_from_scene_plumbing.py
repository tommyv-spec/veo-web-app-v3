"""v698A many-to-one — assert the whole travel path, do not trust it.

The chain is eight links and every one of them has silently dropped a field
before in this codebase (main.py:257-261 documents the v892.2 case). This test
walks the links it can reach without a database or a browser.
"""
import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import models            # noqa: E402
import image_platform    # noqa: E402
import main              # noqa: E402

FIELD = "audio_from_scene"

# 1. the Clip column exists
assert hasattr(models.Clip, FIELD), "models.Clip is missing the column"

# 2. the ImageSceneAssignment column exists and is serialised
assert hasattr(image_platform.ImageSceneAssignment, FIELD), \
    "ImageSceneAssignment is missing the column"
ip_src = inspect.getsource(image_platform)
assert f'"{FIELD}": self.{FIELD}' in ip_src, \
    "ImageSceneAssignment.to_dict() does not serialise the field"

# 3. BOTH migration lists carry it, for BOTH tables
for table in ("image_scene_assignments", "clips"):
    assert f'("{table}", "{FIELD}",' in ip_src, f"no migration row for {table}"
assert ip_src.count(f'ADD COLUMN {FIELD} INTEGER') >= 2, "sqlite migrations missing"
assert ip_src.count(f'ADD COLUMN IF NOT EXISTS {FIELD} INTEGER') >= 2, \
    "postgres migrations missing"

# 4. the markdown parser emits it, and the promote payload carries it
assert f'"{FIELD}": audio_from_scene,' in ip_src, "parser does not emit the field"
assert f'_asg_{FIELD}' in ip_src, "promote payload does not read the field"

# 5. the assignment writer persists it
assert f"{FIELD}=s.get(\"{FIELD}\")" in ip_src, \
    "assignment creation does not persist the field"

# 6. EVERY pydantic model that declares voiceover_line must also declare this
#    one, or that model silently drops it (the v892.2 failure).
receivers = [m for _n, m in inspect.getmembers(main)
             if inspect.isclass(m) and "voiceover_line" in getattr(m, "model_fields", {})]
assert receivers, "found no pydantic model declaring voiceover_line"
for m in receivers:
    assert FIELD in m.model_fields, (
        f"{m.__name__} declares voiceover_line but not {FIELD} - "
        f"pydantic will drop it (the v892.2 failure)")

# 7. it reaches the Clip row
main_src = inspect.getsource(main)
assert f"{FIELD}={FIELD}_val" in main_src, "not passed into Clip(...)"

# 8. the frontend payload map lists it - a key not listed there never arrives
here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
html = open(os.path.join(here, "static", "index.html"), encoding="utf-8").read()
assert f"{FIELD}:" in html, "static/index.html payload map does not carry the field"

print(f"check_audio_from_scene_plumbing: OK ({len(receivers)} pydantic receivers)")
