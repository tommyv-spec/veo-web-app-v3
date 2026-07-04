"""v821 — plumbing test for dialogue_text_b (reworded Prompt B line).

A full end-to-end import can't be unit-tested in isolation here: the flat-row
builder lives inside `prepare_batch_for_video` (an auth+DB FastAPI endpoint) and
the clip write lives inside a large generate handler. So this test asserts the
PLUMBING instead — the exact key-name contract across the three hops:

    parser  -> dict key "prompt_b_line"
    image_platform flat row reads it, emits key "veo_prompt_b_line"
    main.py reads "veo_prompt_b_line" -> clip.dialogue_text_b

If any of those key names drift (the missing/misspelled-name regression class
that py_compile does not catch), this test fails.
"""

import os
import re

from veo_prompt_overrides import parse_veo_prompts_block
from models import Clip

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BLOCK = '''## Veo 3.1 Final Prompts (per clip)

### Clip 1.1
**Text prompt:**
```
IMMEDIATE ACTION: the man lowers the pale banana as he delivers the line. The fit man says in a rapid, fast-paced knowing voice (American accent), speaking quickly and barely pausing for breath: "your soldier won't wake up like it used to."
```
**Prompt B (policy fallback — reworded line):**
```
IMMEDIATE ACTION: the man lowers the pale banana as he delivers the line. The fit man says in a rapid, fast-paced knowing voice (American accent), speaking quickly and barely pausing for breath: "your drive just isn't what it was."
```
'''

REWORDED = "your drive just isn't what it was."


def _src(name):
    with open(os.path.join(HERE, name), encoding="utf-8") as fh:
        return fh.read()


def test_dataflow_parser_to_clip_field():
    """Drive the real parser output through the exact read/emit expressions
    the two modules use, and confirm the reworded line survives to the field
    that becomes clip.dialogue_text_b."""
    vp = parse_veo_prompts_block(BLOCK)[(1, 1)]
    assert vp["prompt_b_line"] == REWORDED

    # Hop B — image_platform.py flat-row build (verbatim expression).
    flat_row = {"veo_prompt_b_line": (vp or {}).get("prompt_b_line") if vp else None}

    # Hop C — main.py line_data -> clip assignment (verbatim expression).
    line_data = flat_row
    _veo_prompt_b_line = (line_data.get("veo_prompt_b_line") or "").strip() or None
    dialogue_text_b = _veo_prompt_b_line  # clip.dialogue_text_b = _veo_prompt_b_line

    assert dialogue_text_b == REWORDED


def test_clip_has_dialogue_text_b_column():
    assert "dialogue_text_b" in Clip.__table__.columns.keys()
    assert "rendered_prompt_variant" in Clip.__table__.columns.keys()


def test_image_platform_emits_veo_prompt_b_line():
    src = _src("image_platform.py")
    # both the per-line (vp) and silent (silent_vp) flat rows must emit the key
    assert src.count('"veo_prompt_b_line"') >= 2
    assert '(vp or {}).get("prompt_b_line")' in src
    assert '(silent_vp or {}).get("prompt_b_line")' in src


def test_main_reads_key_and_assigns_field():
    src = _src("main.py")
    # main.py must read the SAME key image_platform emits, and assign the field
    assert '_veo_prompt_b_line = (line_data.get("veo_prompt_b_line")' in src
    assert re.search(r"clip\.dialogue_text_b\s*=\s*_veo_prompt_b_line", src)
    # redo-pending worker payload + ClipResponse expose the new fields
    assert '"dialogue_text_b": clip.dialogue_text_b' in src
    assert '"rendered_prompt_variant": clip.rendered_prompt_variant' in src
    assert "dialogue_text_b=c.dialogue_text_b" in src
