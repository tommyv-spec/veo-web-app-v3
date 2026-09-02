#!/usr/bin/env python3
"""Pre-ship mechanical linter for videos/*.md generate-side artifacts.

Consolidates the MECHANICALLY checkable generate-side gates so a build is
never declared done on memory alone. Deep-dives live in template_reference.md;
this only checks what a regex can prove. Judgment gates (v598 hook stack,
v718d anti-platonic, v752 pacing, v586 grammar) still need a human read.

Usage:  python code/verify_video_format.py videos/<file>.md
Exit 0 = all mechanical gates pass. Exit 1 = one or more FAIL.
"""
import os
import re
import sys

EMDASH = "—"


def lint_promptb_gate(clips):
    """v821 — reworded Prompt B mandatory on every dialogue clip.

    Prompt B = Prompt A verbatim EXCEPT the quoted dialogue line, which is
    reworded (different words, same meaning). Pure helper: `clips` is a list
    of dicts with keys a_prompt / a_line / b_prompt / b_line. Returns a list
    of error strings (empty = all clips pass).

    - No a_line (silent / non-dialogue clip) → no B required, skip.
    - a_line present, b_prompt missing → "Prompt B missing".
    - b_prompt present, b_line missing → "Prompt B has no quoted dialogue line".
    - b_line == a_line (stripped) → "Prompt B line identical to A".
    - A-body (a_line blanked) != B-body (b_line blanked) →
      "Prompt B body must match A except the line".

    NOTE: the body-blanking assumes the dialogue line appears EXACTLY ONCE as a
    quoted span in the prompt. If the exact line text also appears elsewhere
    quoted, the body-match can false-negative (rare in the v797 format).
    """
    errs = []
    for i, c in enumerate(clips, 1):
        a_prompt = c.get("a_prompt")
        a_line = c.get("a_line")
        b_prompt = c.get("b_prompt")
        b_line = c.get("b_line")
        if not (a_line and a_line.strip()):
            continue  # silent / non-dialogue — no B required
        if not (b_prompt and b_prompt.strip()):
            errs.append(f"Clip #{i}: Prompt B missing")
            continue
        if not (b_line and b_line.strip()):
            errs.append(f"Clip #{i}: Prompt B has no quoted dialogue line")
            continue
        if b_line.strip() == a_line.strip():
            errs.append(f"Clip #{i}: Prompt B line identical to A")
            continue
        a_body = (a_prompt or "").replace('"' + a_line + '"', '"<LINE>"')
        b_body = (b_prompt or "").replace('"' + b_line + '"', '"<LINE>"')
        if a_body != b_body:
            errs.append(f"Clip #{i}: Prompt B body must match A except the line")
    return errs


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

    # RENDER ZONE = everything a generator can actually see. The §0 Citations
    # Check block and HTML comments are authoring metadata: they are never sent
    # to Banana or Omni, so a token there cannot seed a render. Token scans that
    # exist to stop render-seeding (v808) must run on THIS, not on the raw file
    # — on 2026-08-03 a build's own "no minors anywhere" compliance note
    # hard-failed the v808 check it was asserting compliance with.
    _t_render = re.sub(r"<!--.*?-->", " ", t, flags=re.S)
    _t_render = re.sub(
        r"^##\s+§0 Citations Check.*?(?=^##\s(?!#)|\Z)", " ", _t_render, flags=re.S | re.M)

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
    voiceover_anchor_targets: list[tuple[str, int]] = []  # (scene_no, target_img)
    pair_scenes: list[dict] = []   # v698A many-to-one, resolved after the loop
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
        # v698A many-to-one — `audio_from_scene: N` is the OTHER legal way for a
        # voiceover scene to get its audio: it rides under scene N's spoken clip
        # instead of minting a twin, so it has no anchor by design. Collected
        # here for the resolver pass after the loop.
        audio_from = re.search(
            r"^-\s+\*\*audio_from_scene:\*\*\s*(\d+)\s*$", blk, re.M)
        anchor = re.search(
            r"^-\s+\*\*voiceover_anchor_image:\*\*\s*image_(\d+)", blk, re.M)
        pair_scenes.append({
            "scene_index": int(sn),
            "speaker_mode": sp.group(1).strip() if sp else "",
            "audio_from_scene": int(audio_from.group(1)) if audio_from else None,
            "anchor_node_id": int(anchor.group(1)) if anchor else None,
            "line": scene_lines[0] if scene_lines else "",
        })
        if sp and _speaker_mode(sp.group(1)) == "voiceover":
            if not anchor and not audio_from:
                fails.append(
                    f"v698A: Scene {sn} speaker reads as voiceover but has neither "
                    f"`- **voiceover_anchor_image:** image_N` (mint an audio twin) "
                    f"nor `- **audio_from_scene:** N` (ride under an existing spoken "
                    f"clip) — platform import will reject it (Gate 9). If the voice "
                    f"is diegetic (spoken in-scene, e.g. off-camera interviewer), use "
                    f"an in-scene speaker value instead of 'voiceover'")
            elif anchor:
                voiceover_anchor_targets.append((sn, int(anchor.group(1))))

    # v698A many-to-one — validate every `audio_from_scene: N` with the SAME
    # resolver Phase 3a uses (code/pairing_resolver.py), so lint and job setup
    # cannot drift apart. It rejects: an anchor and audio_from_scene together,
    # a scene that does not exist, a source that does not speak, and a pairing
    # chained through another voiceover scene.
    if pair_scenes:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        try:
            from pairing_resolver import resolve_audio_sources, PairingError
        except ImportError:
            pass    # older checkout without the module; nothing to enforce
        else:
            try:
                resolve_audio_sources(pair_scenes)
            except PairingError as exc:
                fails.append(f"v698A: {exc}")

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
    role_anchor_images: set[int] = set()
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
        if rm and rm.group(1).strip().lower() == "voiceover_anchor":
            role_anchor_images.add(int(h.group(1)))

    # v698A Gate 10 mirror — the platform import HARD-FAILS when a scene points
    # `voiceover_anchor_image: image_N` at an image whose block does NOT carry
    # `- **role:** voiceover_anchor` (platform message: "must have `- **role:**
    # voiceover_anchor` set in its image block (currently role='NONE')"). The
    # anchor target image must be MARKED as the anchor, not just referenced.
    for sn, tgt in voiceover_anchor_targets:
        if tgt not in role_anchor_images:
            fails.append(
                f"v698A: Scene {sn} points voiceover_anchor_image at image_{tgt}, "
                f"but image_{tgt} has no `- **role:** voiceover_anchor` bullet in "
                f"its image block — platform import will reject it (Gate 10). Add "
                f"`- **role:** voiceover_anchor` to the image_{tgt} block")

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

    # --- v808: NO MINORS anywhere in a build (kids/teens/children/babies) ---
    # Adult sexual-health content + a minor anywhere in frame = ad-policy +
    # classifier risk with zero upside (CLAUDE.md §8). Scan the WHOLE build
    # (Ingredients, image prompts, storyboard, Veo prompts). Word-boundary
    # tokens so "cowboy"/"girlfriend" don't trip; "girl(s)"/"boy(s)" alone are
    # WARN (can mean adults — "keep up with these girls"); explicit minor
    # tokens are FAIL. Negative mentions ("no children") also fail — they can
    # seed the render; describe the couple + "no one else in the frame".
    _minor_re = (
        r"\b(?:teen(?:ager)?s?|child(?:ren)?|kids?|toddlers?|bab(?:y|ies)|minors?|"
        r"sons?|daughters?|(?:[1-9]|1[0-7])-year-old)\b")
    # FAIL only on the render zone — a token in §0 or an HTML comment cannot
    # reach a generator. Meta-zone-only hits still WARN so intent stays visible.
    _minor_fail = re.findall(_minor_re, _t_render, re.I)
    if _minor_fail:
        _uniq = sorted({m.lower() for m in _minor_fail})
        fails.append(f"v808: minor-reference token(s) in build: {_uniq} — NO kids/teens anywhere (CLAUDE.md §8); family payoff = the COUPLE only")
    else:
        _minor_meta = re.findall(_minor_re, t, re.I)
        if _minor_meta:
            _uniqm = sorted({m.lower() for m in _minor_meta})
            warns.append(
                f"v808: minor-reference token(s) {_uniqm} appear only in §0 / comments, not in any "
                "rendered field — not a render risk, but confirm the build does not intend a minor")
    _minor_warn = re.findall(r"\b(?:boys?|girls?)\b", t, re.I)
    if _minor_warn:
        warns.append(f"v808: {len(_minor_warn)} 'boy/girl' token(s) — verify they mean ADULTS (e.g. 'these girls' = adult women is fine)")

    # --- Pre-Flight present (v738) ---
    if "## Pre-Flight Checklist" not in t:
        fails.append("v738: missing `## Pre-Flight Checklist` block")
    else:
        for n in range(1, 8):
            if not re.search(rf"^###\s+{n}\.", t, re.M):
                warns.append(f"v738: Pre-Flight section {n} not found")

    # --- Final Prompts section (v750 structure, v865 Omni body) ---
    # v865 — new builds title this `## Google Omni Final Prompts`; the legacy
    # Veo title stays valid (every shipped build uses it).
    _sec_hdr = re.search(
        r"^##\s+(?:Veo\s*3\.?1|Google\s+Omni|Omni)\s+Final\s+Prompts\b.*$",
        t, re.M | re.I,
    )
    _is_omni_section = bool(_sec_hdr and "omni" in _sec_hdr.group(0).lower())
    if not _sec_hdr:
        fails.append("v750/v865: missing `## Google Omni Final Prompts` (or legacy `## Veo 3.1 Final Prompts`) section")
    else:
        veo = t[_sec_hdr.end():]
        if re.search(r"^###\s+Scene\s+\d+\s+—", veo, re.M):
            fails.append("v750: Veo section uses legacy `### Scene N —` headers (use `### Clip N.M`)")
        beats = re.findall(r"\[(?:Start|Mid-clip|End)\s+beat[^\]]*\]", veo)
        if beats:
            fails.append(f"v750: {len(beats)} beat-bracket(s) in Veo prompts (banned; beats live in action_note)")
        # v807 — Veo renders each clip in ISOLATION from its start frame; it
        # never sees the previous clip. Editing/transition language in a clip
        # prompt is noise at best and can make Veo render a cut INSIDE the
        # clip. Describe only what happens within the clip.
        cuts = re.findall(r"\bhard cut\b|\bcuts?\s+(?:to|back|away|from)\b|\btransitions?\s+(?:to|from)\b", veo, re.I)
        if cuts:
            fails.append(f"v807: {len(cuts)} editing/transition phrase(s) in Veo prompts ('hard cut'/'cut to'/...) — describe only what happens INSIDE the clip; the cut between clips is the editor's job, not Veo's")
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
            # v865 — the Omni master block replaces the v718h-A IMMEDIATE
            # ACTION / TERMINAL STATE anchors (operator 2026-07-24). Only new
            # Omni-titled sections are held to the twelve-block Omni shape;
            # legacy Veo-titled builds keep rendering untouched (forward-only).
            if _is_omni_section:
                for _lbl in ("Quality / Fidelity Lock:", "Camera:",
                             "Performance / Action:", "Negative Constraints:"):
                    if _lbl not in blk:
                        warns.append(f"v865: Clip {n}.{m} missing `{_lbl}` block")

    # --- v821: reworded Prompt B mandatory on every dialogue clip ---
    # Prompt B (v821) = Prompt A verbatim EXCEPT the quoted dialogue line,
    # reworded (different words, same meaning). Reuse the SAME per-clip parse
    # the platform import uses (parse_veo_prompts_block) so the linter sees
    # exactly what the importer sees. a_line = last quoted span of the A
    # text_prompt; b_line = prompt_b_line (parser-extracted).
    # A raised exception HARD-FAILS (a broken parse must never let a build slip
    # past the gate). An EMPTY dict is legitimate (a build with no Veo /
    # dialogue clips = nothing to check) and PASSES.
    _clip_map = None
    try:
        from veo_prompt_overrides import parse_veo_prompts_block
        _clip_map = parse_veo_prompts_block(t)
    except Exception as _e:
        fails.append(f"v821 Prompt B gate could not parse Veo clips: {_e}")
    if _clip_map is not None:
        _pb_clips = []
        for (_si, _li), _cd in sorted(_clip_map.items()):
            _a = _cd.get("text_prompt")
            _q = re.findall(r'"([^"]+)"', _a or "")
            # v865 quote-trap — when a prompt carries a spoken line
            # (`saying exactly:`), that line must be the ONLY double-quoted
            # span, because the v821 comparison below reads the LAST quoted
            # span as the dialogue line. A stray quote in Audio / Style /
            # Negative Constraints would silently steal it. A and B are
            # separate parsed strings — check each on its own.
            _b = _cd.get("prompt_b")
            # Restricted to Omni-titled sections only (operator 2026-07-24
            # regression run): legacy Veo-titled builds legitimately carry a
            # second quoted span in some prompts and must stay untouched
            # (forward-only) — only new Omni-shaped prompts are held to the
            # single-quoted-span discipline.
            if _is_omni_section:
                if _a and "saying exactly:" in _a and len(_q) != 1:
                    fails.append(
                        f"v865: Clip {_si}.{_li} Prompt A has {len(_q)} double-quoted spans "
                        "— the spoken line must be the only quoted text (v821 reads the last quoted span)"
                    )
                if _b and "saying exactly:" in _b:
                    _qb = re.findall(r'"([^"]+)"', _b)
                    if len(_qb) != 1:
                        fails.append(
                            f"v865: Clip {_si}.{_li} Prompt B has {len(_qb)} double-quoted spans "
                            "— the spoken line must be the only quoted text (v821 reads the last quoted span)"
                        )
            _pb_clips.append({
                "a_prompt": _a,
                "a_line": _q[-1].strip() if _q else None,
                "b_prompt": _cd.get("prompt_b"),
                "b_line": _cd.get("prompt_b_line"),
            })
        for _e in lint_promptb_gate(_pb_clips):
            fails.append(f"v821: {_e} — reworded Prompt B mandatory on every dialogue clip (see template_reference §v821)")

    # v944/v947 — the ## Finishing section must parse exactly as import will
    # parse it. Same function, so lint-pass == import-pass by construction.
    try:
        from image_platform import parse_finishing_section
        parse_finishing_section(t)
    except ImportError as _fe:
        warns.append(f"v947 finishing: image_platform not importable here ({_fe}) — "
                     f"section NOT checked; import will still enforce it")
    except Exception as _fe:
        # Any raise = FAIL, same convention as the v821 gate above: a broken
        # parse must never crash the linter mid-run (that would swallow every
        # earlier gate's output) nor let a build slip past the gate.
        fails.append(f"v947 finishing: {_fe}")

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
