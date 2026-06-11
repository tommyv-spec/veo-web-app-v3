#!/usr/bin/env python3
"""Pre-ship mechanical linter for videos/*.md generate-side artifacts.

Consolidates the MECHANICALLY checkable generate-side gates so a build is
never declared done on memory alone. Deep-dives live in template_reference.md;
this only checks what a regex can prove. Judgment gates (v598 hook stack,
v718d anti-platonic, v752 pacing, v586 grammar) still need a human read.

Usage:  python code/verify_video_format.py videos/<file>.md
Exit 0 = all mechanical gates pass. Exit 1 = one or more FAIL.
"""
import re
import sys

EMDASH = "—"


def lint(path: str) -> int:
    t = open(path, encoding="utf-8").read()
    fails: list[str] = []
    warns: list[str] = []

    # --- structure / parser (v696 + v594) ---
    images = re.findall(r"^###\s+Image\s+(\d+)", t, re.M)
    scenes = re.findall(r"^###\s+Scene\s+(\d+)\s*$", t, re.M)
    # `### Clip N.M` (dialogue clips) OR `### Clip N — Scene N (silent)`
    # (v682f silent/text_card single-clip form — no .M suffix).
    clips = re.findall(r"^###\s+Clip\s+(\d+)(?:\.(\d+))?", t, re.M)
    img_nums = set(int(i) for i in images)
    if len(images) != len(set(images)):
        fails.append("v594: duplicate `### Image N` numbers")
    if not scenes:
        fails.append("v696: no `### Scene N` blocks found")

    # split scene blocks
    sblocks = re.split(r"(?=^###\s+Scene\s+\d+\s*$)", t, flags=re.M)
    used_imgs: set[int] = set()
    end_frames: list[tuple[int, int]] = []
    line_count = 0
    for b in sblocks:
        h = re.match(r"^###\s+Scene\s+(\d+)\s*$", b, re.M)
        if not h:
            continue
        sn = h.group(1)
        cut = re.search(r"^(?:###\s|##\s)", b[h.end():], re.M)
        blk = b[: h.end() + cut.start()] if cut else b
        is_text_card = "scene_type:** text_card" in blk
        img_m = re.search(r"^-\s+\*\*image:\*\*\s+image_(\d+)", blk, re.M)
        if not is_text_card and not img_m:
            fails.append(f"v696: Scene {sn} missing `- **image:** image_N`")
        if img_m:
            used_imgs.add(int(img_m.group(1)))
        scene_lines = re.findall(r"^-\s+\*\*line:\*\*\s+(.+)$", blk, re.M)
        line_count += len(scene_lines)
        # v681 — `speaker: silent` scenes have no line bullets by design
        # (image_platform.py parse tolerates them; Veo prompt rides the
        # v682f `### Clip N — Scene N (silent)` override block).
        is_silent = re.search(r"\*\*speaker:\*\*\s*silent\b", blk, re.I)
        if not is_text_card and not is_silent and not scene_lines:
            fails.append(f"v696: Scene {sn} has no `- **line:**`")
        if not is_text_card and "**action_note:**" not in blk:
            warns.append(f"v540: Scene {sn} missing action_note")
        # word budget v577 (approx 2.6 x target_duration_s)
        td = re.search(r"^-\s+\*\*target_duration_s:\*\*\s+([\d.]+)", blk, re.M)
        if td:
            budget = 2.6 * float(td.group(1))
            for ln in scene_lines:
                wc = len(ln.split())
                if wc > budget + 0.5:
                    warns.append(f"v577: Scene {sn} line {wc}w > budget {budget:.0f}w")
        ef = re.search(r"^-\s+\*\*end_frame_image:\*\*\s+image_(\d+)", blk, re.M)
        if ef and img_m:
            end_frames.append((int(img_m.group(1)), int(ef.group(1))))

    # end_frame_image validity (v718i)
    for start, end in end_frames:
        if end not in img_nums:
            fails.append(f"v718i: end_frame_image image_{end} not defined")
        if start == end:
            fails.append(f"v718i: end_frame_image == start image_{start} (illegal)")
        used_imgs.add(end)
    # unused images (v594)
    for n in img_nums:
        if n not in used_imgs:
            warns.append(f"v594: Image {n} declared but never used by a scene")

    # --- vocabulary (v693 lowercase + v615 em-dash) ---
    all_lines = re.findall(r"^-\s+\*\*line:\*\*\s+(.+)$", t, re.M)
    for ln in all_lines:
        if any(c.isupper() for c in ln):
            fails.append(f"v693: line not fully lowercase: {ln[:50]!r}")
        if EMDASH in ln or " -- " in ln:
            fails.append(f"v615: em-dash in line: {ln[:50]!r}")

    # --- Pre-Flight present (v738) ---
    if "## Pre-Flight Checklist" not in t:
        fails.append("v738: missing `## Pre-Flight Checklist` block")
    else:
        for n in range(1, 8):
            if not re.search(rf"^###\s+{n}\.", t, re.M):
                warns.append(f"v738: Pre-Flight section {n} not found")

    # --- Veo Final Prompts format (v750) ---
    if "## Veo 3.1 Final Prompts" not in t:
        fails.append("v750: missing `## Veo 3.1 Final Prompts` section")
    else:
        veo = t.split("## Veo 3.1 Final Prompts", 1)[1]
        if re.search(r"^###\s+Scene\s+\d+\s+—", veo, re.M):
            fails.append("v750: Veo section uses legacy `### Scene N —` headers (use `### Clip N.M`)")
        beats = re.findall(r"\[(?:Start|Mid-clip|End)\s+beat[^\]]*\]", veo)
        if beats:
            fails.append(f"v750: {len(beats)} beat-bracket(s) in Veo prompts (banned; beats live in action_note)")
        if "```" in veo:
            # v750.1 (veo_prompt_overrides.py _extract_prompt_content) tries fenced
            # extraction first, falls back to unfenced — so fences still render.
            # Doc-style preference is unfenced; flag as WARN, not a render-blocker.
            warns.append("v750: code fences in Veo section (v750 prefers plain markdown; v750.1 still extracts fenced)")
        for n, m in clips:
            head = rf"^### Clip {n}\.{m}\b" if m else rf"^### Clip {n}\b(?!\.)"
            cb = re.search(head + r".*?(?=^### Clip|\Z)", veo, re.M | re.S)
            if not cb:
                continue
            blk = cb.group(0)
            # Negative prompt dropped per operator 2026-06-04 standing rule
            # (feedback_no-negative-prompts): never include a Negative block.
            for fld in ("Start frame:", "Text prompt:"):
                if f"**{fld}**" not in blk:
                    fails.append(f"v750: Clip {n}.{m} missing **{fld}**")
            if "IMMEDIATE ACTION:" not in blk:
                warns.append(f"v750: Clip {n}.{m} missing IMMEDIATE ACTION:")
            if "TERMINAL STATE:" not in blk:
                warns.append(f"v750: Clip {n}.{m} missing TERMINAL STATE:")

    # --- report ---
    print(f"FILE: {path}")
    print(f"  Images={len(images)}  Scenes={len(scenes)}  Clips={len(clips)}  Lines={line_count}")
    for w in warns:
        print(f"  WARN  {w}")
    for f in fails:
        print(f"  FAIL  {f}")
    if fails:
        print(f"RESULT: FAIL ({len(fails)} blocking, {len(warns)} warn)")
        return 1
    print(f"RESULT: PASS ({len(warns)} warn)")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python code/verify_video_format.py videos/<file>.md")
        sys.exit(2)
    sys.exit(lint(sys.argv[1]))
