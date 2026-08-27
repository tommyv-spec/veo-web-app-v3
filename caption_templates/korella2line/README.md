# korella2line — the korella look with a HARD 2-line cap

Byte-for-byte the `korella` template plus one field:

```json
"on_text_overflow_strategy": "exceed_width"
```

## Why it exists

`korella` already says `"max_number_of_lines": 2`, and it renders 3-line cards
anyway. That is not a bug in the template — in pycaps the cap is only honoured
when the overflow strategy asks for it. The default is
`EXCEED_MAX_NUMBER_OF_LINES` ("if the text does not fit, add a line"), so
`max_number_of_lines` is never consulted
(`pycaps/layout/line_splitter.py:28`, `pycaps/layout/definitions.py:38`).
`exceed_width` flips the trade: the last line takes the remaining words and is
allowed to run wider than `max_width_ratio` instead of wrapping again.

## What it buys, and what it costs

Measured on the cupping job `eb23f66d` (2026-08-27):

- **buys** a caption band of a constant ~224px (0.117 of frame) instead of one
  that swells to 352px (0.183) exactly where a face or a prop is. That is the
  whole reason a middle-band placement becomes usable at all.
- **costs** two things. The last line can run wider than 0.75 of frame — on
  that job the widest measured line was 0.83, but a card that keeps its
  original grouping ran to 0.99, edge to edge. And re-running the line splitter
  re-groups the caption cards (same words, same timings, different split).

So this is not a drop-in upgrade for `korella`. It is a second template on
purpose: pick it when the band has to stay a known height, keep `korella` when
card grouping and the width ratio matter more.

`korella` is deliberately NOT modified — every other account renders through it.

## Using it

```
python code/send_to_platform.py ... --template korella2line \
    --placement constant --autoedit-offset 0.08
```

The template name flows through untouched (`AutoEditRequest.template` is a free
string; the worker passes `run["template"]` to `run_autoedit`), and
`autoedit_pipeline.local_styles()` discovers any directory here that holds a
`pycaps.template.json`.

**Not yet reachable from an installed worker.** The worker bootstrap allow-list
in `code/main.py` (~L19766) and its installer scripts (~L20362, L20425, L20521)
name the three `korella` files literally, so a worker installed that way never
receives this directory. Extending those lists is the remaining
productionization step.
