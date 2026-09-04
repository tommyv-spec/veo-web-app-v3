"""v959 — the import linter mirrors the parser, and stands down where it must.

`code/verify_video_format.py` is the gate that `tools/run_build_checks.py` runs
on every build, so it has to say the same YES/NO the platform parser says about
a movie-section scene: 1-2 face refs, named only with the method, an 8 or 10
second window, exactly one line, and all shot scenes movie-section or none.

The other half of this file is the stand-down. A movie-section clip carries the
mentor's own prompt shape — a verbatim `Setting:` paragraph, timestamped beats
and one tail line — so the checks that encode OUR clip grammar must not fire on
it: the v807 transition scan (it tripped on his wardrobe words "with the sleeves
cut away"), the v865 twelve-block house shape, the v750 fence preference and the
v577 word budget. v594 counts a face ref as a real use of an image. Everything
that is compliance or contract keeps running on a section build — the tests at
the bottom pin the Prompt B gate as the witness for that half.

Lives in its own file (not test_movie_section_render_method.py) because it tests
a different program: the linter script, not the platform parser.
"""
import contextlib
import io
import pathlib
import sys

_HERE = pathlib.Path(__file__).parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


# --- fixtures (same shapes as test_movie_section_render_method.py) -----------

LEGACY_SCENE = """### Scene 1

- **image:** image_1
- **speaker:** on-camera
- **line:** american men over sixty are doing this every morning
- **clip_duration_s:** 6
- **action_note:** she lifts the jar [Start beat]
"""

SECTION_SCENE = """### Scene 1

- **image:** image_1
- **render_method:** movie-section
- **face_refs:** image_2, image_3
- **speaker:** on-camera
- **line:** wow if my husband looked like you i would never leave the house then he should do what i do
- **clip_duration_s:** 10
- **action_note:** she watches him lift the sack one-handed [Start beat]
"""

# A card is drawn by ffmpeg, never rendered as a clip, so it takes no render
# method. It rides beside a real section scene here so the build stays legal.
TEXT_CARD_SCENE = """### Scene 2

- **scene_type:** text_card
- **caption:** the end
- **bg_color:** black
"""

MIN_BUILD = """# t
## Pre-Flight Checklist
### 1. x
## Images
### Image 1
- **Image prompt:**
```
wide two-shot in a hardware store loading lot, both people head to toe
```
### Image 2
- **reference_image:** image_1
- **Image prompt:**
```
close-up of the man
```
### Image 3
- **reference_image:** image_1
- **Image prompt:**
```
close-up of the woman
```
## Storyboard
{scenes}
## Google Omni Final Prompts
### Clip 1.1
**Text prompt:**
```
Setting: The loading lot.

00:00 - 00:04
[She stares.]

Woman:
"wow if my husband looked like you i would never leave the house"

00:04 - 00:06
[He grins.]

Man:
"then he should do what i do"

camera switches between faces more often
```
**Prompt B (policy fallback):**
```
Setting: The loading lot.

00:00 - 00:04
[She stares.]

Woman:
"wow if my husband looked like you i would never leave the house"

00:04 - 00:06
[He grins.]

Man:
"then he ought to do what i do"

camera switches between faces more often
```
"""


def _lint(md, tmp_path):
    import verify_video_format as v
    p = tmp_path / "b.md"
    p.write_text(md, encoding="utf-8")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = v.lint(str(p))
    return code, buf.getvalue()


def _findings(out):
    """Only the WARN / FAIL lines.

    The report's first line is the file path, and pytest names the temp folder
    after the test — so a test called `..._v807_...` finds "v807" in its own
    path and passes when the gate is still firing. Read the findings, not the
    whole report.
    """
    return "\n".join(ln for ln in out.splitlines()
                     if ln.strip().startswith(("WARN", "FAIL")))


# --- 1. the linter mirrors the parser's hard-fails ---------------------------

def test_linter_accepts_a_movie_section_build(tmp_path):
    code, out = _lint(MIN_BUILD.format(scenes=SECTION_SCENE), tmp_path)
    assert "v959" not in _findings(out)


def test_linter_tolerates_a_trailing_comma_in_face_refs(tmp_path):
    # The parser drops empty tokens (image_platform.py ~L6075), so a trailing
    # comma is a typo the import forgives — the linter must forgive it too.
    md = MIN_BUILD.format(
        scenes=SECTION_SCENE.replace("image_2, image_3", "image_2, image_3,"))
    code, out = _lint(md, tmp_path)
    assert "v959" not in _findings(out)


def test_linter_fails_face_refs_without_method(tmp_path):
    md = MIN_BUILD.format(scenes=LEGACY_SCENE + "- **face_refs:** image_2\n")
    code, out = _lint(md, tmp_path)
    assert code != 0 and "v959" in out and "face_refs" in out


def test_linter_fails_mixed_build(tmp_path):
    md = MIN_BUILD.format(
        scenes=SECTION_SCENE + "\n" + LEGACY_SCENE.replace("### Scene 1", "### Scene 2"))
    code, out = _lint(md, tmp_path)
    assert code != 0 and "all shot scenes" in out


def test_linter_fails_bad_window(tmp_path):
    md = MIN_BUILD.format(
        scenes=SECTION_SCENE.replace("- **clip_duration_s:** 10", "- **clip_duration_s:** 6"))
    code, out = _lint(md, tmp_path)
    assert code != 0 and "clip_duration_s" in out


def test_linter_fails_duplicate_face_ref(tmp_path):
    md = MIN_BUILD.format(scenes=SECTION_SCENE.replace("image_2, image_3", "image_2, image_2"))
    code, out = _lint(md, tmp_path)
    assert code != 0 and "repeats" in out


def test_linter_fails_an_unknown_render_method(tmp_path):
    md = MIN_BUILD.format(
        scenes=SECTION_SCENE.replace("- **render_method:** movie-section",
                                     "- **render_method:** omni-section"))
    code, out = _lint(md, tmp_path)
    assert code != 0 and "is not charswap or movie-section" in _findings(out)


def test_linter_fails_zero_face_refs(tmp_path):
    md = MIN_BUILD.format(
        scenes=SECTION_SCENE.replace("- **face_refs:** image_2, image_3\n", ""))
    code, out = _lint(md, tmp_path)
    assert code != 0 and "face_refs must name 1-2" in _findings(out)


def test_linter_fails_three_face_refs(tmp_path):
    # A fourth image so the count is the ONLY thing wrong with this build.
    build = MIN_BUILD.replace(
        "## Storyboard",
        "### Image 4\n- **Image prompt:**\n```\nclose-up of the friend\n```\n## Storyboard")
    md = build.format(
        scenes=SECTION_SCENE.replace("image_2, image_3", "image_2, image_3, image_4"))
    code, out = _lint(md, tmp_path)
    assert code != 0 and "face_refs must name 1-2" in _findings(out)


def test_linter_fails_a_face_ref_that_is_not_image_n(tmp_path):
    md = MIN_BUILD.format(
        scenes=SECTION_SCENE.replace("image_2, image_3", "image_2, faces/man.png"))
    code, out = _lint(md, tmp_path)
    found = _findings(out)
    assert code != 0 and "face_refs must name 1-2" in found and "faces/man.png" in found


def test_linter_fails_an_undefined_face_ref(tmp_path):
    md = MIN_BUILD.format(
        scenes=SECTION_SCENE.replace("image_2, image_3", "image_2, image_9"))
    code, out = _lint(md, tmp_path)
    assert code != 0 and "face_refs image_9 not defined" in _findings(out)


def test_linter_fails_the_scenes_own_image_as_a_face_ref(tmp_path):
    # image_1 is the scene's wide anchor; a face chip is a close-up, never it.
    md = MIN_BUILD.format(
        scenes=SECTION_SCENE.replace("image_2, image_3", "image_1, image_2"))
    code, out = _lint(md, tmp_path)
    assert code != 0 and "face_refs image_1 is the scene's own image" in _findings(out)


def test_linter_fails_two_lines_on_a_section_scene(tmp_path):
    md = MIN_BUILD.format(
        scenes=SECTION_SCENE + "- **line:** then he should do what i do\n")
    code, out = _lint(md, tmp_path)
    assert code != 0 and "needs exactly one" in _findings(out)


def test_linter_fails_a_render_method_on_a_text_card(tmp_path):
    md = MIN_BUILD.format(
        scenes=SECTION_SCENE + "\n" + TEXT_CARD_SCENE
        + "- **render_method:** movie-section\n")
    code, out = _lint(md, tmp_path)
    assert code != 0 and "text_card scenes take no render_method" in _findings(out)


def test_linter_fails_swap_bullets_on_a_section_scene(tmp_path):
    md = MIN_BUILD.format(
        scenes=SECTION_SCENE + "- **swap_source_video:** raw/refs/curls.mp4\n"
        + "- **swap_mode:** image-led\n")
    code, out = _lint(md, tmp_path)
    assert code != 0 and (
        "does not take swap_source_video / swap_mode" in _findings(out))


# --- 2. the stand-downs (one test per gate) ----------------------------------

# His Setting paragraph describes a shirt "with the sleeves cut away". The v807
# scan looks for `cuts? (to|back|away|from)`, so those three wardrobe words read
# as an edit instruction and hard-failed the whole build (Task 9's first run).
_SLEEVES = "Setting: The loading lot. He wears a grey shirt with the sleeves cut away."


def test_v807_stands_down_on_a_section_build(tmp_path):
    md = MIN_BUILD.format(scenes=SECTION_SCENE).replace(
        "Setting: The loading lot.", _SLEEVES)
    code, out = _lint(md, tmp_path)
    assert "v807" not in _findings(out)


def test_v807_still_fails_a_legacy_build(tmp_path):
    md = MIN_BUILD.format(scenes=LEGACY_SCENE).replace(
        "[He grins.]", "[He grins.] the camera cuts to his face")
    code, out = _lint(md, tmp_path)
    assert code != 0 and "v807" in _findings(out)


def test_v594_counts_face_refs_as_used(tmp_path):
    # Image 2 and Image 3 are named ONLY by `face_refs`; that is a real use.
    code, out = _lint(MIN_BUILD.format(scenes=SECTION_SCENE), tmp_path)
    assert "never used" not in _findings(out)
    # …and the same two images ARE unused on a build that never names them.
    code, out = _lint(MIN_BUILD.format(scenes=LEGACY_SCENE), tmp_path)
    assert "v594: Image 2 declared but never used" in _findings(out)


def test_v865_house_blocks_stand_down_on_a_section_clip(tmp_path):
    code, out = _lint(MIN_BUILD.format(scenes=SECTION_SCENE), tmp_path)
    assert "v865" not in _findings(out)
    # The same clip, rendered our way, still owes the twelve blocks.
    code, out = _lint(MIN_BUILD.format(scenes=LEGACY_SCENE), tmp_path)
    assert "v865: Clip 1.1 missing `Camera:` block" in _findings(out)


def test_v750_fence_warn_stands_down_on_a_section_build(tmp_path):
    code, out = _lint(MIN_BUILD.format(scenes=SECTION_SCENE), tmp_path)
    assert "v750: code fences" not in _findings(out)
    code, out = _lint(MIN_BUILD.format(scenes=LEGACY_SCENE), tmp_path)
    assert "v750: code fences" in _findings(out)


def test_v577_budget_stands_down_on_a_section_line(tmp_path):
    # A section line is the whole section, so the per-clip word budget cannot
    # judge it — §5b's words-per-second gate does, in the auditor.
    md = MIN_BUILD.format(
        scenes=SECTION_SCENE.replace(
            "- **clip_duration_s:** 10",
            "- **clip_duration_s:** 10\n- **target_duration_s:** 4"))
    code, out = _lint(md, tmp_path)
    assert "v577" not in _findings(out)
    md = MIN_BUILD.format(
        scenes=LEGACY_SCENE.replace(
            "- **clip_duration_s:** 6",
            "- **clip_duration_s:** 6\n- **target_duration_s:** 1"))
    code, out = _lint(md, tmp_path)
    assert "v577" in _findings(out)


# --- 3. everything that is compliance or contract still runs -----------------

def test_prompt_b_gate_still_runs_on_a_section_build(tmp_path):
    md = MIN_BUILD.format(scenes=SECTION_SCENE)
    md = md[: md.index("**Prompt B (policy fallback):**")]
    code, out = _lint(md, tmp_path)
    assert code != 0 and "v821" in out and "Prompt B missing" in out
