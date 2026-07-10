"""v827 — the promote path must never fabricate a last frame.

Pre-v827, `promote_batch_to_video` stamped `last_frame_index = len(nodes) - 1`
into the job's dialogue_json. main.py / worker.py then attached that image as
the END frame of the LAST clip, regardless of `clip_mode: fresh` /
`transition: cut` — contradicting v782 ("on a correct fresh/cut build,
end_fname is none for every clip except explicit end_frame_image morphs").

Symptom: the closing clip morphed into whatever the last `### Image N` block
happened to be — a product shot, a comment screenshot, an overlay still.

These are source-level guards, not behaviour tests: the end-frame resolver is
inline in a ~200-line prompt-build loop and cannot be imported in isolation.
They fail loudly if someone reintroduces the fabrication or drops a bound.
"""
import ast
import os
import re
import sys

CODE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(name):
    with open(os.path.join(CODE, name), encoding="utf-8") as fh:
        return fh.read()


# 1) promote must not stamp a computed last_frame_index.
ip_src = _read("image_platform.py")
assert "\"last_frame_index\": len(nodes)" not in ip_src, (
    "image_platform.py: promote is fabricating last_frame_index again "
    "(v827 regression) — it must emit None"
)
assert "\"last_frame_index\": None" in ip_src, (
    "image_platform.py: promote no longer emits an explicit "
    "\"last_frame_index\": None — did the payload change shape?"
)
print("OK v827: promote emits last_frame_index=None")


# 2) The promote payload literal really parses to None (not a string "None",
#    not a stray truthy value). Walk the AST to the dialogue_json dict.
tree = ast.parse(ip_src)
found = []
for node in ast.walk(tree):
    if not isinstance(node, ast.Dict):
        continue
    for k, v in zip(node.keys, node.values):
        if isinstance(k, ast.Constant) and k.value == "last_frame_index":
            found.append(v)
assert found, "image_platform.py: no dict literal with a last_frame_index key"
for v in found:
    assert isinstance(v, ast.Constant) and v.value is None, (
        f"image_platform.py: last_frame_index literal is {ast.dump(v)}, expected None"
    )
print(f"OK v827: all {len(found)} last_frame_index literal(s) are None")


# 3) Both resolvers must reject a negative index. `lfi < num_images` alone let
#    a stored -1 index the last frame from the end of the list.
for fname, needle in (
    ("main.py", r"0 <= lfi < num_images"),
    ("worker.py", r"0 <= lfi < num_images"),
    ("worker.py", r"0 <= last_frame_index < len\(images\)"),
):
    src = _read(fname)
    assert re.search(needle, src), (
        f"{fname}: missing lower bound guard /{needle}/ — a negative "
        f"last_frame_index would index from the end of the frame list"
    )
print("OK v827: both resolvers bound last_frame_index to [0, num_images)")


# 4) The explicit-operator-pick path must survive. If someone "fixes" this by
#    gating on clip_mode == 'blend', the manual storyboard editor's End Frame
#    drag target silently stops working.
main_src = _read("main.py")
assert "request.last_frame_index" in main_src, (
    "main.py: POST /api/jobs no longer reads request.last_frame_index — the "
    "manual-upload End Frame picker is broken"
)
print("OK v827: manual-upload End Frame picker still wired")

print("ALL v827 last_frame_index checks pass")
