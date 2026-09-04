"""v960.2 — the auto-edit worker's install set carries EVERY local caption
template, read off the disk instead of typed out in two places that had to agree.

The bug this closes: the download allow-list and the two installer scripts both
listed `korella` alone. `korella2line` and `garnissa` (v960) were added as
directories, which is all the RUNNING worker needs because `local_styles()`
reads the directory — so a freshly INSTALLED worker could render neither, and it
would have surfaced as "template not found" on somebody else's machine long
after the template was declared working.

Run: python tests/check_v960_2_worker_template_files.py   (from code/)
"""
import os
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, str(ROOT))

import main  # noqa: E402

TEMPLATES = ROOT / "caption_templates"
# the same test local_styles() uses, so this test cannot drift from the pipeline
on_disk = sorted(d.name for d in TEMPLATES.iterdir()
                 if (d / "pycaps.template.json").exists())
assert len(on_disk) >= 3, on_disk
print(f"templates on disk: {', '.join(on_disk)}")

files = main._autoedit_caption_template_files()
covered = sorted({name.split("/")[0] for name, _sub, _rel in files})
assert covered == on_disk, f"served {covered}, on disk {on_disk}"
print("OK every template on disk is served — none is left behind")

# each entry points at a file that really exists, under code/, and nowhere else
for name, sub, rel in files:
    p = ROOT / rel
    assert p.exists(), f"{name} -> {rel} does not exist"
    assert ROOT in p.resolve().parents, f"{name} escapes code/"
    assert name.count("/") == 1, f"download name must be '<template>/<basename>': {name}"
    assert name.split("/")[1] == p.name
    assert sub.startswith("caption_templates\\")
    # the font sits one level deeper, and the installer has to say so or it
    # lands beside styles.css where pycaps will not find it
    assert sub.endswith("\\resources") == (p.parent.name == "resources"), name
print("OK every entry resolves to a real file inside code/, with the right install folder")

names = [n for n, _s, _r in files]
assert len(names) == len(set(names)), "two entries share a download name"
assert not any("README" in n for n in names), "README.md is documentation, not a runtime file"
print("OK names are unique and documentation is not shipped")

# the installers and the allow-list are now built from the SAME source
worker_files = main._autoedit_worker_files()
wnames = [n for n, _s in worker_files]
assert len(wnames) == len(set(wnames))
assert set(names) <= set(wnames), "the installers dropped a template file"
mods = [n for n, _s in main.AUTOEDIT_WORKER_FILES]
assert set(mods) <= set(wnames) and "autoedit_pipeline.py" in mods
assert len(worker_files) == len(mods) + len(files)
print(f"OK installers ship {len(worker_files)} files = {len(mods)} modules + {len(files)} template files")

# the route's allow-list accepts each one and still refuses everything else
src = open(ROOT / "main.py", encoding="utf-8").read()
i = src.find("async def serve_autoedit_worker_file(")
body = src[i:src.find("\n@app.", i)]
assert "for _name, _sub, _rel in _autoedit_caption_template_files():" in body, \
    "the route must build its allow-list from the derived files"
assert 'ALLOWED[_name] = _rel' in body
assert "rel = ALLOWED.get(name)" in body and "raise HTTPException(404" in body, \
    "it must stay an allow-list — a path parameter may never reach the filesystem"
assert 'Path(__file__).parent / rel' in body
# no hand-typed korella rows left to drift
assert '"korella/styles.css": Path(' not in body, "a typed-out template row survived"
print("OK the route builds its allow-list from the same source and stays an allow-list")

# both installers read the derived list, not the bare module list
for m in re.finditer(r"for fn, sub in (\w+)", src):
    assert m.group(1) == "_autoedit_worker_files", \
        f"an installer still loops over {m.group(1)} and will miss every template"
assert src.count("for fn, sub in _autoedit_worker_files()") == 2, \
    "expected exactly two installers (Windows + unix)"
print("OK both installers loop over the derived list")

print("ALL OK check_v960_2_worker_template_files")
