"""v961 — the per-clip render model: allowlist, parser and linter agree.

The point of these tests is that FOUR places name the same five model strings
(veo_models.py, the linter's standalone fallback, the worker's dropdown selector
map, and the worker's own default). Any two of them drifting is a job that
renders on a model nobody chose, with nothing failing.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import veo_models                      # noqa: E402
import verify_video_format as vvf      # noqa: E402

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_v961_linter_models_match_the_parser():
    """The linter's standalone fallback must equal the canonical allowlist."""
    assert tuple(vvf._V961_ALLOWED_VEO_MODELS) == tuple(veo_models.ALLOWED_VEO_MODELS)


def test_v961_worker_dropdown_offers_every_legal_model():
    """Every legal model must have a selector the worker can actually click.

    A model that passes validation but has no dropdown entry is worse than a
    rejected one: the job proceeds on whatever model was already selected.
    """
    fw = open(os.path.join(HERE, "static", "flow_worker.py"), encoding="utf-8").read()
    for model in veo_models.ALLOWED_VEO_MODELS:
        assert f'"{model}": [' in fw, (
            f"{model!r} is legal but flow_worker has no MODEL_SELECTORS entry "
            f"for it — the dropdown pick would fall through to the default")


def test_v961_worker_default_matches_the_canonical_default():
    """The worker's local DEFAULT_VEO_MODEL copy must equal the server's."""
    fw = open(os.path.join(HERE, "static", "flow_worker.py"), encoding="utf-8").read()
    m = re.search(r'^DEFAULT_VEO_MODEL = "([^"]+)"', fw, re.M)
    assert m, "flow_worker has no module-level DEFAULT_VEO_MODEL"
    assert m.group(1) == veo_models.DEFAULT_VEO_MODEL


def test_v961_only_omni_offers_ingredients():
    """v943 charswap and v959 sections fail closed off Omni; the helper that
    encodes that must agree with the models list."""
    assert veo_models.offers_ingredients_tab("Omni Flash")
    for model in veo_models.ALLOWED_VEO_MODELS:
        if model != "Omni Flash":
            assert not veo_models.offers_ingredients_tab(model)


def test_v961_validation_is_exact_and_case_sensitive():
    assert veo_models.is_legal_veo_model("Veo 3.1 - Lite [Lower Priority]")
    assert veo_models.is_legal_veo_model("Veo 3.1 - Lite")
    # the two Lite variants are DIFFERENT dropdown options, not aliases
    assert "Veo 3.1 - Lite" in veo_models.ALLOWED_VEO_MODELS
    assert "Veo 3.1 - Lite [Lower Priority]" in veo_models.ALLOWED_VEO_MODELS
    # near misses must be refused, never repaired
    for bad in ("omni flash", "Omni", "Veo 3.1 Lite", "veo 3.1 - lite",
                "Veo 3.1 - Lite ", "", None, 4, "Lite"):
        if bad == "Veo 3.1 - Lite ":
            # trailing space is normalised away first, then legal
            assert veo_models.is_legal_veo_model(
                veo_models.normalize_veo_model(bad))
            continue
        assert not veo_models.is_legal_veo_model(bad), f"{bad!r} was accepted"


def test_v961_conflict_check_only_constrains_the_forced_clips():
    """A charswap clip in the job must NOT veto a legal model on ordinary clips.

    The first implementation compared one job-wide forced model against EVERY
    clip spec, so a single charswap clip rejected `Lite` on an unrelated silent
    cutaway — defeating v961 on exactly the mixed jobs it exists for. Found by
    an adversarial review of the implementation, 2026-09-05.
    """
    src = open(os.path.join(HERE, "image_platform.py"), encoding="utf-8").read()
    i = src.index("_v961_forced = _v943_model or _v959_model")
    block = src[i:i + 2000]
    assert "_v961_needs_ingredients" in block, (
        "the v961 conflict check does not filter to the clips that actually "
        "need the Ingredients tab — a job-wide comparison rejects legal models "
        "on ordinary clips")
    # the comprehension must be gated on that predicate
    assert "if _v961_needs_ingredients(spec)" in block, \
        "the conflict comprehension is not gated on the render_method predicate"
    # and the predicate must cover BOTH arms
    assert '"charswap"' in block and "MOVIE_SECTION_RENDER_METHOD" in block, \
        "the predicate must match charswap AND movie-section"


def test_v961_normalize_returns_none_for_empty():
    assert veo_models.normalize_veo_model(None) is None
    assert veo_models.normalize_veo_model("") is None
    assert veo_models.normalize_veo_model("   ") is None
    assert veo_models.normalize_veo_model(" Omni Flash ") == "Omni Flash"


def test_v961_linter_rejects_an_unknown_model(tmp_path):
    """A build declaring a model the dropdown cannot find must FAIL the lint."""
    build = tmp_path / "b.md"
    build.write_text(
        "## Storyboard\n\n"
        "### Scene 1\n"
        "- **image:** image_1\n"
        "- **line:** hello there\n"
        "- **veo_model:** Veo 4 Turbo\n",
        encoding="utf-8")
    out = vvf.lint(str(build))
    assert out != 0, "linter accepted an unknown veo_model"


def test_v961_linter_accepts_a_legal_model(tmp_path):
    """And a legal one must not be the reason a build fails."""
    build = tmp_path / "b.md"
    build.write_text(
        "## Storyboard\n\n"
        "### Scene 1\n"
        "- **image:** image_1\n"
        "- **line:** hello there\n"
        "- **veo_model:** Veo 3.1 - Lite [Lower Priority]\n",
        encoding="utf-8")
    # the fixture is not a complete build, so it may fail for other reasons —
    # what must NOT appear is a v961 complaint. Match the FAIL line, not the
    # bare token: pytest names tmp_path after the test, so the printed FILE:
    # path contains "v961" on every run and a bare substring check is always
    # true (this test failed on exactly that before the assertion was tightened).
    import io
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        vvf.lint(str(build))
    offending = [ln for ln in buf.getvalue().splitlines()
                 if "v961:" in ln and ("FAIL" in ln or "WARN" in ln)]
    assert not offending, offending
