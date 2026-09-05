"""v961 per-clip veo_model — assert the whole travel path, do not trust it.

Written RED, before the implementation, on purpose.

Why this file exists: v698A.2.1 is the standing proof that a per-clip field can
exist on the row, be read by the worker, and still be inert on every job because
one hand-maintained dict in the middle never carried it. Nothing failed; the
feature was simply switched off and nobody could see it.

This field is worse than average for that failure because the payload sites are
SPLIT BY WORKER TYPE: three local-worker dicts and two user-worker dicts. Miss
the user-worker pair and the model override works on one worker and silently
no-ops on the other, which reads as a flaky worker rather than a missing field.
The Codex review of the v961 plan caught exactly that omission in the draft.

Run: python code/tests/check_veo_model_plumbing.py
"""
import ast
import inspect
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import veo_models       # noqa: E402
import models           # noqa: E402
import image_platform   # noqa: E402
import main             # noqa: E402

FIELD = "veo_model"

# ── 1. the allowlist lives in ONE dependency-neutral module ──────────────────
# NOT in main.py: main.py imports image_platform (main.py:166), and
# image_platform would have to import the constant back — a cycle. This mirrors
# ALLOWED_CLIP_DURATIONS_S living in the neutral clip_duration.py.
assert hasattr(veo_models, "ALLOWED_VEO_MODELS"), \
    "veo_models.ALLOWED_VEO_MODELS is missing"
assert isinstance(veo_models.ALLOWED_VEO_MODELS, tuple), \
    "ALLOWED_VEO_MODELS must be an immutable tuple"
for _m in ("Omni Flash", "Veo 3.1 - Quality", "Veo 3.1 - Fast",
           "Veo 3.1 - Lite", "Veo 3.1 - Lite [Lower Priority]"):
    assert _m in veo_models.ALLOWED_VEO_MODELS, f"missing legal model {_m!r}"

# the neutral module must NOT import the application entry point
vm_src = inspect.getsource(veo_models)
assert "import main" not in vm_src, "veo_models must stay dependency-neutral"

# ── 2. the Clip column exists ────────────────────────────────────────────────
assert hasattr(models.Clip, FIELD), "models.Clip is missing the veo_model column"

# ── 3. BOTH migration rows exist ─────────────────────────────────────────────
# The clips-table migrations live in image_platform.py, not models.py: sqlite at
# ~:215 and postgres at ~:431. Both lists must carry the column or one backend
# starts throwing on a column the ORM believes exists.
ip_src = inspect.getsource(image_platform)
assert f'("clips", "{FIELD}",' in ip_src, "no clips.veo_model migration row"
assert f"ALTER TABLE clips ADD COLUMN {FIELD} VARCHAR" in ip_src, \
    "sqlite migration missing"
assert f"ALTER TABLE clips ADD COLUMN IF NOT EXISTS {FIELD} VARCHAR" in ip_src, \
    "postgres migration missing"

# ── 3b. THE ASSIGNMENT ROW MUST CARRY IT TOO ─────────────────────────────────
# The column on `clips` is NOT enough. BOTH promote paths — prepare_batch_for_video
# (the batch/browser route) and promote_batch_to_video (the CLI route) — build
# their per-scene dicts from ImageSceneAssignment ROWS, not from the markdown.
# A value that lives only in the parser output is therefore always None by the
# time a Clip row is written: the bullet parses, validates, passes every gate,
# and reaches the worker as NULL.
#
# This is not hypothetical. `explicit_target_s` had exactly this bug until
# v889.1 ("Until the assignment row gained the column this was ALWAYS None, so
# the override never fired and the anchor gap won silently"), and v961 shipped
# with it too — caught 2026-09-05 by tracing the promote path of a real batch,
# after the auditor, the linter, the platform check and this very file were all
# green. Hence this section.
assert hasattr(image_platform.ImageSceneAssignment, FIELD), \
    ("ImageSceneAssignment is missing the veo_model column — both promote paths "
     "read scenes from these rows, so the per-clip model would always be NULL "
     "on the Clip (the v889.1 / v698A.2.1 failure)")
assert f'"{FIELD}": self.{FIELD}' in ip_src, \
    "ImageSceneAssignment.to_dict() does not serialise veo_model"
assert f'("image_scene_assignments", "{FIELD}",' in ip_src, \
    "no image_scene_assignments.veo_model migration row"
assert f"ALTER TABLE image_scene_assignments ADD COLUMN {FIELD} VARCHAR" in ip_src, \
    "sqlite migration missing for image_scene_assignments"
assert f"ALTER TABLE image_scene_assignments ADD COLUMN IF NOT EXISTS {FIELD} VARCHAR" in ip_src, \
    "postgres migration missing for image_scene_assignments"
assert f'{FIELD}=s.get("{FIELD}")' in ip_src, \
    "assignment creation does not persist veo_model from the parsed scene"
assert f'"{FIELD}": next((m for m in clip_veo_models if m), None)' in ip_src, \
    "the parser does not emit a SCENE-level veo_model for the assignment row"
assert f'getattr(_assignment, "{FIELD}", None)' in ip_src, \
    ("promote_batch_to_video does not read veo_model off the assignment row — "
     "its clip_specs would carry an always-absent key, making the v961 conflict "
     "check decorative and the Clip row NULL")

# ── 4. the markdown parser reads the bullet and resolves it ──────────────────
assert f"{FIELD}" in ip_src, "image_platform does not mention veo_model"
assert "ALLOWED_VEO_MODELS" in ip_src, \
    "image_platform does not validate against the allowlist"
# v943/v959 conflict must HARD-FAIL at import, not at render time
assert "v961" in ip_src, "image_platform carries no v961 marker"

# ── 5. THE FIVE PER-CLIP PAYLOAD DICTS ───────────────────────────────────────
# Anchored on veo_render_duration_s: it is the analogous per-clip override and
# it is present at every site this field must also reach. Counting its sites and
# requiring the same count for ours is what makes a NEW payload path (added
# later, by someone else) fail this test instead of silently dropping the field.
main_src = inspect.getsource(main)
dur_sites = main_src.count('"veo_render_duration_s": clip.veo_render_duration_s')
ours = main_src.count(f'"{FIELD}": clip.{FIELD}')
assert dur_sites >= 5, (
    f"expected >=5 veo_render_duration_s payload sites, found {dur_sites} — "
    "the anchor moved; re-derive the site list before trusting this test")
assert ours >= dur_sites, (
    f"veo_render_duration_s appears in {dur_sites} per-clip payload dicts but "
    f"{FIELD} in only {ours}. EVERY payload that carries the duration override "
    "must carry the model override, INCLUDING the two user-worker dicts "
    "(main.py ~:20315 and ~:20552) — otherwise the model override works on "
    "local-worker jobs and is silently inert on user-worker jobs (the "
    "v698A.2.1 failure, split by worker type)")

# ── 6. both ClipResponse constructors carry it ───────────────────────────────
resp_dur = main_src.count("veo_render_duration_s=c.veo_render_duration_s")
resp_ours = main_src.count(f"{FIELD}=c.{FIELD}")
assert resp_ours >= resp_dur, (
    f"ClipResponse carries veo_render_duration_s {resp_dur}x but {FIELD} "
    f"{resp_ours}x — the review page and clip_qc would read None")

# ── 7. the field whitelists list it ──────────────────────────────────────────
# A key absent from these lists is dropped without a word.
for _ in range(1):
    wl_dur = main_src.count('"veo_render_duration_s",')
    wl_ours = main_src.count(f'"{FIELD}",')
    assert wl_ours >= wl_dur, (
        f"veo_render_duration_s is whitelisted {wl_dur}x, {FIELD} only "
        f"{wl_ours}x — an unlisted key is silently dropped")

# ── 8. it reaches the Clip row ───────────────────────────────────────────────
assert f"{FIELD}={FIELD}" in main_src or f"{FIELD}=clip_{FIELD}" in main_src, \
    "not passed into Clip(...)"

# ── 8b. NO DICT LITERAL MAY CARRY "veo_model" TWICE ──────────────────────────
# The count check above is textual, and a text count cannot tell a live entry
# from a DEAD one. Two of the redo payload dicts already carried a job-level
# "veo_model" key; inserting a per-clip key into the same literal left BOTH, and
# Python keeps the LAST — so clip.veo_model was silently discarded on those
# paths while the count check happily counted it. Found by an adversarial review
# of the implementation, 2026-09-05, not by this test.
#
# Parse the AST instead of the text: a duplicate key is now impossible to ship.
for _node in ast.walk(ast.parse(main_src)):
    if isinstance(_node, ast.Dict):
        _keys = [k.value for k in _node.keys
                 if isinstance(k, ast.Constant) and isinstance(k.value, str)]
        assert _keys.count(FIELD) <= 1, (
            f"main.py line {_node.lineno}: dict literal carries {FIELD!r} "
            f"{_keys.count(FIELD)} times — Python keeps only the LAST, so the "
            f"earlier one is dead code that this file's count check would still "
            f"count as plumbed")

# ── 9. BOTH worker submit loops set the per-clip model BEFORE the tab ────────
# _omni_ingredients_mode reads page._veo_model at call time, and the tab is
# chosen by set_clip_input_mode. If the model is set after, the tab is computed
# from the PREVIOUS clip's model while the dropdown gets the right one.
#
# There are TWO live submit loops in flow_worker.py. Applying the model in only
# one leaves the other rendering on the job model with nothing failing — which
# is what the first implementation did.
here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
fw = open(os.path.join(here, "static", "flow_worker.py"), encoding="utf-8").read()
assert "v961" in fw, "flow_worker carries no v961 marker"

# The SUBMIT functions, by name. `analyze_clip_chains` also iterates clips but
# renders nothing, so it is deliberately not in this list. Naming them beats
# pattern-matching every `for i, clip in enumerate(clips):` — that also matched
# the analysis loop, and matching `set_clip_input_mode(` alone also matched two
# docstrings and the `def` line.
_SUBMIT_FNS = ("process_job_submission_with_failover", "process_job_submission")
_fn_starts = {}
for _fn in _SUBMIT_FNS:
    m = re.search(r"^def %s\(" % re.escape(_fn), fw, re.M)
    assert m, f"submit function {_fn} not found — re-derive this check"
    _fn_starts[_fn] = m.start()

for _fn, _start in _fn_starts.items():
    _later = [p for p in
              [m.start() for m in re.finditer(r"^def ", fw, re.M)] if p > _start]
    _seg = fw[_start:min(_later + [len(fw)])]
    _ia = _seg.find("apply_clip_veo_model(page, clip, veo_model,")
    assert _ia != -1, (
        f"{_fn} never calls apply_clip_veo_model — that submit path renders "
        f"every clip on the JOB model, silently, whatever the build declared. "
        f"There is more than one submit loop in this file and the first v961 "
        f"implementation only covered one of them.")
    _it = _seg.find("set_clip_input_mode(page,")
    if _it != -1:
        assert _ia < _it, (
            f"{_fn}: apply_clip_veo_model must come BEFORE set_clip_input_mode "
            "— _omni_ingredients_mode reads page._veo_model at call time, so "
            "setting it later computes the Frames/Ingredients tab from the "
            "PREVIOUS clip's model")

print("check_veo_model_plumbing: OK "
      f"({ours} payload dicts, {resp_ours} ClipResponse sites, "
      f"{len(veo_models.ALLOWED_VEO_MODELS)} legal models)")
