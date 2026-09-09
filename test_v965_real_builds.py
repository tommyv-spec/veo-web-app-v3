"""v965 end-to-end on REAL builds from videos/, with no render spent.

WHY THIS FILE EXISTS SEPARATELY FROM test_clip_contract.py
----------------------------------------------------------
That file proves the pieces with synthetic scenes it wrote itself, which is
exactly the weakness the operator named: a test that builds its own input can
agree with a parser that is wrong in the same way. This one takes REAL builds
off disk, stamps a COPY of them in memory, and walks the whole chain:

    real videos/*.md
      -> _parse_scene_blocks_new            (does it parse, and stamp?)
      -> ClipContractDeclaration            (is the declaration readable back?)
      -> a Clip row carrying that string    (what the platform would store)
      -> _v965_attach_contract              (the resolved contract, per lane)
      -> the worker's ledger                (what the arms would be told)

Nothing is written to videos/ and no job is created. The real files are opened
read-only; the contract bullets are inserted into an in-memory copy.

WHAT THIS CANNOT TELL YOU. It does not prove a render. It proves that a real
build, authored the way the rule says, survives every hop with the values it
declared. The render proof is stage 3 and costs real clips.
"""
import copy
import io
import json
import pathlib
import re
import sys

import pytest

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_HERE = pathlib.Path(__file__).parent
_ROOT = _HERE.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import clip_contract as cc  # noqa: E402
import image_platform as ip  # noqa: E402

VIDEOS = _ROOT / "videos"

MOVIE_SECTION_BUILD = (
    "nuri-korella-ed-county-fair-hay-bale-rancher64-wife-admires-husband-"
    "asks-healer-beside-morning-jar-movie-section-comment-growth-v1.md")
SIMPLE_BUILD = (
    "nuri-korella-ed-laziest-way-soldier-two-step-capsule-lemon-cinnamon-"
    "proof-stack-korella-saffron-selling-v1.md")


# --------------------------------------------------------------------------
# helpers — stamp a real build in memory, exactly as an author would on disk
# --------------------------------------------------------------------------

def _read(name):
    p = VIDEOS / name
    if not p.exists():                                    # pragma: no cover
        pytest.skip(f"real build not on disk: {name}")
    return p.read_text(encoding="utf-8")


def _image_indexes(md):
    return set(int(m) for m in re.findall(r"^###\s+Image\s+(\d+)", md, re.M))


def _stamp(md, input_mode="ingredients", isolate="true",
           ladder="prompt_b, fail"):
    """Add the §0 opt-in and the three bullets to every SHOT scene.

    Inserted after each scene's `- **line:**`, which is where they attach —
    the same rule v861's clip_duration_s follows. A text_card scene is left
    alone on purpose; the rule refuses the bullets there.
    """
    out, in_scene, is_card = [], False, False
    for line in md.splitlines():
        if line.startswith("### Scene "):
            in_scene, is_card = True, False
        elif line.startswith("## ") or line.startswith("### Image"):
            in_scene = False
        if in_scene and "scene_type:**" in line and "text_card" in line:
            is_card = True
        out.append(line)
        if in_scene and not is_card and line.strip().startswith("- **line:**"):
            out.append(f"- **input_mode:** {input_mode}")
            out.append(f"- **isolate_project:** {isolate}")
            out.append(f"- **policy_fallback:** {ladder}")
    return "CLIP CONTRACT: v1\n" + "\n".join(out)


class _Clip:
    """A Clip row carrying what the platform would have stored for this scene."""

    def __init__(self, scene, job_id="job-real", method=None):
        self.id = f"clip-{scene.get('scene_index')}"
        self.job_id = job_id
        self.clip_contract_json = scene.get("clip_contract_json")
        self.clip_contract_version = scene.get("clip_contract_version")
        self.render_method = method
        self.start_frame = f"jobs/{job_id}/frames/image_1.png"
        self.end_frame = None
        self.swap_mode = None
        self.swap_source_r2_key = None
        self.swap_avatar_upload_id = None
        self.face_ref_frames_json = None
        self.veo_model = "Omni Flash"
        self.veo_render_duration_s = 8
        self.aspect_ratio = "9:16"
        self.flow_variants_count = 2
        self.resolution = "720p"


def _worker_ledger(contract):
    """Run the SHIPPED worker code, read out of flow_worker.py as text."""
    src = (_HERE / "static" / "flow_worker.py").read_text(encoding="utf-8")
    ns = {"os": __import__("os"), "json": json,
          "datetime": __import__("datetime").datetime,
          "_hashlib": __import__("hashlib"), "__file__": str(_HERE)}
    consts = src[src.index("\nV965_APPLY = False"):src.index("\ndef v965_write_diag(")]
    exec(consts, ns)                                       # noqa: S102
    start = src.index("\ndef v965_build_ledger(")
    rest = src[start + 1:]
    exec(rest[:rest.index("\ndef ", 1)], ns)               # noqa: S102
    return ns["v965_build_ledger"](contract)


# --------------------------------------------------------------------------
# 1. the real builds parse today, untouched, and are NOT in scope
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name", [MOVIE_SECTION_BUILD, SIMPLE_BUILD])
def test_a_real_build_untouched_is_out_of_scope(name):
    """The no-metadata contract. Nothing changes for a build that has not
    opted in, and 347 of 347 are in that state today."""
    md = _read(name)
    scenes = ip._parse_scene_blocks_new(md, _image_indexes(md))
    assert scenes, "the real build produced no scenes"
    assert all(s.get("clip_contract_version") is None for s in scenes)
    assert all(s.get("clip_contract_json") is None for s in scenes)


def test_every_real_build_on_disk_still_parses():
    """The whole corpus, not a sample. This is the check that would catch a
    parser change breaking somebody's existing video."""
    ok = failed = stamped = 0
    for p in sorted(VIDEOS.glob("*.md")):
        md = p.read_text(encoding="utf-8", errors="replace")
        try:
            scenes = ip._parse_scene_blocks_new(md, _image_indexes(md))
            ok += 1
            stamped += sum(1 for s in scenes if s.get("clip_contract_version"))
        except Exception:
            failed += 1
    assert failed == 0, f"{failed} real build(s) stopped parsing"
    assert stamped == 0, "a build opted itself in without saying so"
    assert ok >= 300, f"only {ok} builds parsed — did videos/ move?"


# --------------------------------------------------------------------------
# 2. stamp a real build and walk the whole chain
# --------------------------------------------------------------------------

def test_a_real_simple_build_carries_its_contract_end_to_end():
    md = _stamp(_read(SIMPLE_BUILD), input_mode="frames", isolate="false")
    scenes = ip._parse_scene_blocks_new(md, _image_indexes(md))
    shot = [s for s in scenes if s.get("scene_type") != "text_card"
            and s.get("clip_contract_version")]
    assert shot, "no shot scene was stamped in a real in-scope build"

    from main import _v965_attach_contract

    for scene in shot:
        d = cc.ClipContractDeclaration.model_validate_json(
            scene["clip_contract_json"])
        assert d.input_mode == "frames"
        assert d.isolate_project is False

        clip = _Clip(scene)
        payload = _v965_attach_contract({}, clip, "https://h", "local-worker")
        contract = payload["clip_contract"]

        # what the author declared survived to the worker's payload
        assert contract["input_mode"] == "frames"
        assert contract["isolate_project"] is False
        # and the declaration travelled byte-unchanged beside it
        assert payload["clip_contract_declared"] == scene["clip_contract_json"]

        # the worker can account for every field it was sent
        ledger = _worker_ledger(contract)
        assert {r["field"] for r in ledger} == set(contract)
        assert not [r for r in ledger if r["action"] == "unknown-field"]


def test_a_real_movie_section_build_resolves_its_faces_in_order():
    """The lane where the old worker inferred 'scene vs face' from whether a
    url appeared in face_ref_urls. Here the roles are declared."""
    md = _stamp(_read(MOVIE_SECTION_BUILD), input_mode="ingredients")
    scenes = ip._parse_scene_blocks_new(md, _image_indexes(md))
    shot = [s for s in scenes if s.get("clip_contract_version")]
    assert shot, "the real movie-section build stamped no scene"

    from main import _v965_attach_contract

    scene = shot[0]
    clip = _Clip(scene, method="movie-section")
    clip.face_ref_frames_json = json.dumps([
        f"jobs/{clip.job_id}/frames/ms_face_0_0.png",
        f"jobs/{clip.job_id}/frames/ms_face_0_1.png"])
    contract = _v965_attach_contract({}, clip, "https://h",
                                     "local-worker")["clip_contract"]

    assert [a["role"] for a in contract["assets"]] == [
        "start_frame", "face", "face"]
    assert [a["origin"] for a in contract["assets"][1:]] == [
        "ms_face_0_0.png", "ms_face_0_1.png"]
    assert all(a["media"] == "image" for a in contract["assets"])


def test_a_real_build_missing_one_bullet_is_refused_by_name():
    """The failure an author will actually hit. The message has to say which
    scene and which bullet, because it is their only feedback."""
    md = _stamp(_read(SIMPLE_BUILD))
    md = md.replace("- **isolate_project:** true\n", "", 1)
    with pytest.raises(ValueError) as e:
        ip._parse_scene_blocks_new(md, _image_indexes(md))
    msg = str(e.value)
    assert "isolate_project" in msg and "CLIP CONTRACT: v1" in msg
    assert re.search(r"Scene \d+", msg), "the refusal does not name the scene"


def test_a_real_movie_section_build_cannot_share_a_project():
    """isolate_project: false on a section scene is a hard fail, because the
    worker's own submit proof depends on one submitter per project."""
    md = _stamp(_read(MOVIE_SECTION_BUILD), input_mode="ingredients",
                isolate="false")
    with pytest.raises(ValueError) as e:
        ip._parse_scene_blocks_new(md, _image_indexes(md))
    msg = str(e.value)
    assert "isolate_project" in msg
    assert "movie-section" in msg
    assert "submit proof" in msg, "the refusal should say WHY, not just no"


# --------------------------------------------------------------------------
# 3. the gap, asserted so it stays visible
# --------------------------------------------------------------------------

def test_there_is_still_no_charswap_build_to_test_against():
    """Stage 3 needs one real clip per lane and NO charswap build exists, so
    that lane is unproven on real content. This test documents the hole and
    fails the day someone authors one, which is the moment to extend this file
    rather than discover the gap at the gate."""
    charswap = [p for p in VIDEOS.glob("*.md")
                if "render_method:** charswap" in p.read_text(
                    encoding="utf-8", errors="replace")]
    assert not charswap, (
        f"a charswap build now exists ({[p.name for p in charswap]}) — add it "
        f"to this file and to PLAN.md step 2.14")
