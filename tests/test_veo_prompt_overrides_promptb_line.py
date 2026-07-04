from veo_prompt_overrides import parse_veo_prompts_block

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


def test_prompt_b_line_extracted():
    # parse_veo_prompts_block keys results by (scene_idx, line_idx) tuple.
    result = parse_veo_prompts_block(BLOCK)[(1, 1)]
    assert result["prompt_b"]
    assert result["prompt_b_line"] == "your drive just isn't what it was."
