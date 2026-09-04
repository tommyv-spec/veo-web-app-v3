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

# v959 — the section window and the face-reference cap. Literals on purpose:
# this script has to lint with no platform import available. The guard against
# them drifting from image_platform.MOVIE_SECTION_WINDOWS_S /
# MOVIE_SECTION_MAX_FACE_REFS is a test —
# test_movie_section_linter.py::test_v959_numbers_match_the_parser.
V959_WINDOWS_S = (8, 10)
V959_MAX_FACE_REFS = 2

# v961 — the legal per-clip render models. Imported from the ONE source of
# truth when this linter runs beside the app, with a literal fallback for the
# standalone case (this file is run directly against a build .md by
# run_build_checks, sometimes from a checkout without the app on sys.path).
# The fallback drifting from veo_models.ALLOWED_VEO_MODELS is caught by
# test_v961_linter_models_match_the_parser.
try:
    from veo_models import ALLOWED_VEO_MODELS as _V961_ALLOWED_VEO_MODELS
except ImportError:  # pragma: no cover - standalone lint
    _V961_ALLOWED_VEO_MODELS = (
        "Omni Flash",
        "Veo 3.1 - Quality",
        "Veo 3.1 - Fast",
        "Veo 3.1 - Lite",
        "Veo 3.1 - Lite [Lower Priority]",
    )


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
    # v959 — one row per SHOT scene: (scene number, its render_method or None).
    # text_card scenes never appear here; they carry no render method.
    shot_methods: list[tuple[str, str | None]] = []
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
        # v959 — read the render method FIRST: it decides which of our own
        # clip-grammar checks below apply to this scene at all.
        rm = re.search(r"^-\s+\*\*render_method:\*\*\s*(\S+)", blk, re.M)
        rm_val = rm.group(1).strip().lower() if rm else None
        is_section = rm_val == "movie-section"
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
        # v959 — skipped on a movie-section scene: its one line holds the WHOLE
        # section (every speaker, in order), so a per-clip budget cannot judge
        # it. The words-per-second gate of §5b judges it instead (auditor).
        td = re.search(r"^-\s+\*\*target_duration_s:\*\*\s+([\d.]+)", blk, re.M)
        if td and not is_section:
            budget = 2.6 * float(td.group(1))
            for ln in scene_lines:
                wc = len(ln.split())
                if wc > budget + 0.5:
                    warns.append(f"v577: Scene {sn} line {wc}w > budget {budget:.0f}w")
        ef = re.search(r"^-\s+\*\*end_frame_image:\*\*\s+image_(\d+)", blk, re.M)
        if ef and img_m:
            end_frames.append((int(img_m.group(1)), int(ef.group(1))))
        # v961 — the per-clip render model, mirrored from image_platform.py so a
        # build that lints here is a build the platform parser accepts. Exact,
        # case-sensitive: the strings are the Flow dropdown's own labels, and
        # `Veo 3.1 - Lite` and `Veo 3.1 - Lite [Lower Priority]` are DIFFERENT
        # options. A model the dropdown cannot find does not fail loudly at
        # render time — it leaves the job on whatever was already selected.
        for _vm_raw in re.findall(r"^-\s+\*\*veo_model:\*\*\s*(.+?)\s*$", blk, re.M):
            if _vm_raw.strip() not in _V961_ALLOWED_VEO_MODELS:
                fails.append(
                    f"v961: Scene {sn} veo_model {_vm_raw.strip()!r} is not one of "
                    + " | ".join(_V961_ALLOWED_VEO_MODELS))
        # v959 — movie-section declarations, mirrored from image_platform.py so
        # a build that lints here is a build the platform parser accepts.
        fr = re.search(r"^-\s+\*\*face_refs:\*\*\s*(.+)$", blk, re.M)
        if rm_val and rm_val not in ("charswap", "movie-section"):
            fails.append(f"v943/v959: Scene {sn} render_method {rm_val!r} is not charswap or movie-section")
        if fr and not is_section:
            fails.append(f"v959: Scene {sn} has face_refs but render_method is not movie-section")
        # The swap bullets belong to charswap alone: a movie-section scene
        # renders from images, so there is no source video to swap into.
        _swaps = [_b for _b in ("swap_source_video", "swap_mode")
                  if re.search(rf"^-\s+\*\*{_b}:\*\*", blk, re.M)]
        if is_section and _swaps:
            fails.append(f"v959: Scene {sn} render_method=movie-section does not take "
                         f"swap_source_video / swap_mode (found {_swaps})")
        # Same reason for `- **audio:**` — it re-muxes the swap source's OWN
        # track onto the exported segment, and a section scene has no swap
        # source to take one from (image_platform.py :6022-6041).
        if is_section and re.search(r"^-\s+\*\*audio:\*\*", blk, re.M):
            fails.append(f"v959: Scene {sn} `- **audio:**` only means something on a charswap "
                         f"scene — a movie-section scene has no swap source either, so drop the bullet")
        # A text_card is drawn by ffmpeg, never rendered as a clip, so a render
        # method on one has nothing to act on. Said FIRST and alone, the way the
        # parser raises it (image_platform.py :6048), so the author reads the
        # real problem instead of being asked for face chips a card cannot use.
        # Scoped to movie-section because that is where the parser draws the
        # line: a charswap text_card with its full trio parses today (measured
        # 2026-09-04), and a linter stricter than the parser is a false FAIL.
        if is_section and is_text_card:
            fails.append(f"v959: Scene {sn} — text_card scenes take no render_method")
        elif is_section:
            # An empty token is a trailing comma, not a ref: the parser drops
            # those (image_platform.py :6075) and so must this.
            refs = [x.strip() for x in fr.group(1).split(",") if x.strip()] if fr else []
            ref_nums = [int(m.group(1)) for m in
                        (re.fullmatch(r"image_(\d+)", r) for r in refs) if m]
            _cap = V959_MAX_FACE_REFS
            _s5c = "(v959, wiki/concepts/prompting/movie-style-prompting.md §5c)"
            if not (1 <= len(ref_nums) <= _cap) or len(ref_nums) != len(refs):
                fails.append(f"v959: Scene {sn} face_refs must name 1-{_cap} `image_N` (got {refs})")
            if len(set(ref_nums)) != len(ref_nums):
                fails.append(f"v959: Scene {sn} face_refs repeats an image ({refs}) — the parser refuses it too")
            for n in ref_nums:
                if n not in img_nums:
                    fails.append(f"v959: Scene {sn} face_refs image_{n} not defined — "
                                 f"there is no `### Image {n}` block {_s5c}")
                if img_m and n == int(img_m.group(1)):
                    fails.append(f"v959: Scene {sn} face_refs image_{n} is the scene's own image — "
                                 f"a face chip is a close-up, the scene chip is the wide anchor; "
                                 f"they cannot be the same file {_s5c}")
                # A face ref IS a use of that image — it is uploaded and
                # attached as a chip, so v594 must not call it unused.
                used_imgs.add(n)
            if len(scene_lines) != 1:
                fails.append(f"v959: Scene {sn} movie-section needs exactly one `- **line:**` (found {len(scene_lines)})")
            # The window, read the way the parser reads it (image_platform.py
            # :6164-6197 + :6277). Two traps live here: the value must be a
            # BARE integer (`8.0` is refused, and a leading-digit read would
            # call it 8), and the bullet ATTACHES TO THE LINE ABOVE IT, so one
            # written before the line is dangling and the section has no window.
            _bul = re.findall(r"^-\s+\*\*(line|clip_duration_s):\*\*\s*(.+?)\s*$", blk, re.M)
            _win_raw, _seen_line = None, False
            for _k, _v in _bul:
                if _k == "line":
                    _seen_line, _win_raw = True, None
                elif _seen_line:
                    _win_raw = _v
            _windows = " or ".join(str(w) for w in V959_WINDOWS_S)
            if _win_raw is None and re.search(r"^-\s+\*\*clip_duration_s:\*\*", blk, re.M):
                fails.append(f"v959: Scene {sn} clip_duration_s must follow the line it belongs to — "
                             f"the parser attaches it to the line above, so this one is dangling "
                             f"and the section reads no window")
            elif _win_raw is None:
                fails.append(f"v959: Scene {sn} movie-section needs `- **clip_duration_s:** {_windows}` (got none)")
            elif not re.fullmatch(r"\d+", _win_raw) or int(_win_raw) not in V959_WINDOWS_S:
                fails.append(f"v959: Scene {sn} movie-section needs `- **clip_duration_s:** {_windows}` "
                             f"as a bare integer (got {_win_raw!r})")
        if not is_text_card:
            shot_methods.append((sn, rm_val))
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

    # v959 — all shot scenes or none (two coherent systems, never mixed).
    _ms = [sn for sn, m in shot_methods if m == "movie-section"]
    if _ms and len(_ms) != len(shot_methods):
        _other = [sn for sn, m in shot_methods if m != "movie-section"]
        fails.append(f"v959: render_method=movie-section must cover all shot scenes or none "
                     f"(declared on scene {', '.join(_ms)}; missing on scene {', '.join(_other)})")
    # A SECTION BUILD is one where every shot scene renders the mentor's way.
    # On it, the checks that encode OUR clip grammar stand down further down —
    # his prompt shape (`Setting:` + timestamped beats + one tail line) breaks
    # them by design. Compliance and contract checks never stand down.
    _is_section_build = bool(shot_methods) and all(
        m == "movie-section" for _, m in shot_methods)
    section_scene_nums = {int(sn) for sn in _ms}

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
        # v959 — a movie-section clip is EXEMPT: it ships his `Setting:`
        # paragraph verbatim, and on 2026-09-04 the words "with the sleeves cut
        # away" (a shirt, not an edit) hard-failed a whole build here. So the
        # scan reads the section text with every movie-section clip block cut
        # out; a text_card or legacy clip in the same file is still read.
        # The cut-out ends at the next `### Clip`, the next `## ` section, or
        # EOF — without the `## ` bound the LAST clip's block would run to the
        # end of the file and exempt every section after it (a `## Captions`
        # block saying "cuts to" would go unread).
        # The head pattern below tolerates any spacing after `###`, so the
        # terminator has to as well: with a literal `^### Clip` here, a build
        # whose headers carry two spaces starts a cut-out that never ends and
        # swallows the whole rest of the file.
        _v807_zone = veo
        if section_scene_nums:
            _skip: list[tuple[int, int]] = []
            for n, m in clips:
                if int(n) not in section_scene_nums:
                    continue
                head = rf"^###\s+Clip\s+{n}\.{m}\b" if m else rf"^###\s+Clip\s+{n}\b(?!\.)"
                cb = re.search(head + r".*?(?=^###\s+Clip|^##\s|\Z)", veo, re.M | re.S)
                if cb:
                    _skip.append(cb.span())
            if _skip:
                _kept, _prev = [], 0
                for _a, _b in sorted(_skip):
                    _kept.append(veo[_prev:_a])
                    _prev = _b
                _kept.append(veo[_prev:])
                _v807_zone = "".join(_kept)
        cuts = re.findall(r"\bhard cut\b|\bcuts?\s+(?:to|back|away|from)\b|\btransitions?\s+(?:to|from)\b", _v807_zone, re.I)
        if cuts:
            fails.append(f"v807: {len(cuts)} editing/transition phrase(s) in Veo prompts ('hard cut'/'cut to'/...) — describe only what happens INSIDE the clip; the cut between clips is the editor's job, not Veo's")
        # v959 — his clip prompt IS a fenced block in every shipped example, so
        # the unfenced house preference does not apply to a section build. This
        # one is build-level, not per-clip, because the check reads the whole
        # section at once ("``` in veo") — there is no per-clip form of it.
        if "```" in veo and not _is_section_build:
            # v750.1 (veo_prompt_overrides.py _extract_prompt_content) tries fenced
            # extraction first, falls back to unfenced — so fences still render.
            # Doc-style preference is unfenced; flag as WARN, not a render-blocker.
            warns.append("v750: code fences in Veo section (v750 prefers plain markdown; v750.1 still extracts fenced)")
        for n, m in clips:
            # Same three bounds as the v807 cut-out above: the next clip, the
            # next `## ` section, or EOF. Without the `## ` bound the LAST
            # clip's block ran to the end of the file, so a `**Text prompt:**`
            # or a house block sitting in a later section (`## Captions`,
            # `## Notes`) counted as that clip's own and the checks below
            # passed on text the clip never carried.
            #
            # The head has to tolerate the same spacing `clips` does (it is
            # built with `\s+` up at the top). With a literal single space here,
            # a `###  Clip 1.2` header IS in `clips` but its block never
            # matched — and `continue` then skipped every v750 and v865 check
            # for that clip in silence. A header that the file lists must be a
            # header this loop can read.
            head = rf"^###\s+Clip\s+{n}\.{m}\b" if m else rf"^###\s+Clip\s+{n}\b(?!\.)"
            cb = re.search(head + r".*?(?=^###\s+Clip|^##\s|\Z)", veo, re.M | re.S)
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
            # v959 — a movie-section clip carries NONE of the house blocks on
            # purpose (§5 law 4: his clip prompts carry no quality lock, no
            # camera block, no negative list). The auditor's
            # c_v959_movie_section checks his shape instead.
            if _is_omni_section and int(n) not in section_scene_nums:
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
