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


def _flat(token: str) -> str:
    return "".join(ch for ch in token if ch.isalpha())


def _classify_speaker_token(flat: str) -> str | None:
    # EXACT mirror of image_platform._normalize_speaker_mode token sets.
    if flat in ("oncamera", "dialogue", "speaks", "spoken", "lipsync",
                "character", "characterspeaks"):
        return "on-camera"
    if flat in ("voiceover", "vo", "narration", "offscreen", "narrator",
                "narrated"):
        return "voiceover"
    if flat in ("silent", "mute", "nodialogue", "nospeech",
                "music", "musiconly", "sfx", "sfxonly",
                "broll", "brolloverlay"):
        return "silent"
    if flat in ("auto", "detect", "default"):
        return "auto"
    return None


def _speaker_mode(raw: str) -> str | None:
    # Mirror of the platform's 3-step normalize: whole string → last token
    # → per-token with priority voiceover > on-camera > silent > auto.
    s = raw.strip().lower()
    if not s:
        return None
    c = _classify_speaker_token(_flat(s))
    if c:
        return c
    parts = s.rsplit(None, 1)
    if len(parts) >= 2:
        c = _classify_speaker_token(_flat(parts[-1]))
        if c:
            return c
    found = set()
    for tok in s.split():
        c = _classify_speaker_token(_flat(tok))
        if c:
            found.add(c)
    for mode in ("voiceover", "on-camera", "silent", "auto"):
        if mode in found:
            return mode
    return None


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
        # v698A Gate 9 mirror — the platform import HARD-FAILS a scene whose
        # speaker normalizes to 'voiceover' but has no voiceover_anchor_image.
        # Decode docs write `speaker: voiceover` + `voiceover_anchor_image: none`
        # for diegetic off-camera voices; builds must NOT copy that — use an
        # in-scene speaker value instead (e.g. "the wife in-scene (diegetic)").
        sp = re.search(r"^-\s+\*\*speaker:\*\*\s*(.+)$", blk, re.M)
        if sp and _speaker_mode(sp.group(1)) == "voiceover":
            anchor = re.search(
                r"^-\s+\*\*voiceover_anchor_image:\*\*\s*image_(\d+)", blk, re.M)
            if not anchor:
                fails.append(
                    f"v698A: Scene {sn} speaker reads as voiceover but has no "
                    f"`- **voiceover_anchor_image:** image_N` — platform import "
                    f"will reject it (Gate 9). If the voice is diegetic "
                    f"(spoken in-scene, e.g. off-camera interviewer), use an "
                    f"in-scene speaker value instead of 'voiceover'")

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

    # v698A role vocabulary mirror — the platform import HARD-FAILS any
    # `- **role:**` value other than voiceover_anchor. `role: broll` is
    # DECODE-side annotation only; builds must not carry it (b-roll images
    # are plain images, no role bullet).
    iblocks = re.split(r"(?=^###\s+Image\s+\d+)", t, flags=re.M)
    for b in iblocks:
        h = re.match(r"^###\s+Image\s+(\d+)", b, re.M)
        if not h:
            continue
        cut = re.search(r"^(?:###\s|##\s)", b[h.end():], re.M)
        blk = b[: h.end() + cut.start()] if cut else b
        rm = re.search(r"^\s*[-*]\s*\*\*role:\*\*\s*(.+?)\s*$", blk, re.M)
        if rm and rm.group(1).strip().lower() != "voiceover_anchor":
            fails.append(
                f"v698A: Image {h.group(1)} has role={rm.group(1).strip()!r} — "
                f"platform import only accepts role=voiceover_anchor. "
                f"Delete the role bullet (b-roll images carry no role)")

    # --- vocabulary (v693 lowercase + v615 em-dash) ---
    all_lines = re.findall(r"^-\s+\*\*line:\*\*\s+(.+)$", t, re.M)
    for ln in all_lines:
        if any(c.isupper() for c in ln):
            fails.append(f"v693: line not fully lowercase: {ln[:50]!r}")
        if EMDASH in ln or " -- " in ln:
            fails.append(f"v615: em-dash in line: {ln[:50]!r}")

    # --- v806: image-attached dialogue classifier tokens ---
    # Every clip is image-attached (start frame), so the spoken line runs
    # through Flow's stricter anti-deepfake sexual-content classifier. These
    # tokens PASS text-only but BLOCK with any image attached (operator A/B
    # 2026-07-02). Prompt B does NOT save a token trip (it keeps the dialogue).
    # HARD-FAIL on evidenced tokens; WARN on same-class watch-list.
    _V806_BANNED = ("down there", "wake up harder")
    _V806_WATCH = ("below the waist", "morning wood", "get it up")
    for ln in all_lines:
        low = ln.lower()
        for tok in _V806_BANNED:
            if tok in low:
                fails.append(f"v806: banned image-attached token {tok!r} in line: {ln[:50]!r} (reword keeping function; see template_reference §v806)")
        for tok in _V806_WATCH:
            if tok in low:
                warns.append(f"v806: watch-list token {tok!r} in line: {ln[:50]!r} (same class as evidenced trips)")

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
