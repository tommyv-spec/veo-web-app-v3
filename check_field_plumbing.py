#!/usr/bin/env python3
"""check_field_plumbing.py — v892.6

Catch the fault that produced v892.1, v892.2 and v892.5 in one week: a field
that exists at SOME of the boundaries it has to cross, and is silently dropped
at the one that was missed.

Every boundary in this platform enumerates its fields BY HAND — the pydantic
model, the promote payload in index.html, the apply-block, the response's
`changed_fields` tuple, an assignment's to_dict(). Any such list drifts from
the behaviour it describes, and the symptom always surfaces far away: a render
with no background layer, or a PATCH that answers `changed_fields: []` while
having written the field.

DERIVED, NOT ENUMERATED. This checker holds no list of field names. It reads
each surface out of the source and compares them, so a new field starts being
checked the moment it is added. A hand-maintained list of hand-maintained lists
would be the same bug one level up.

Exit 0 = the surfaces agree. Exit 1 = drift.
"""
import ast
import re
import sys
from pathlib import Path

# `--root <dir>` points the checker at a copy of the tree. That exists so the
# checker can be NEGATIVE-TESTED against deliberately broken copies without
# touching the real files — a check nobody has watched fail is not evidence.
_ROOT_ARG = None
for _i, _a in enumerate(sys.argv[1:]):
    if _a == "--root" and _i + 2 <= len(sys.argv) - 1:
        _ROOT_ARG = sys.argv[_i + 2]
    elif _a.startswith("--root="):
        _ROOT_ARG = _a.split("=", 1)[1]

HERE = Path(_ROOT_ARG).resolve() if _ROOT_ARG else Path(__file__).resolve().parent
MAIN = HERE / "main.py"
INDEX = HERE / "static" / "index.html"
IMGP = HERE / "image_platform.py"

# Fields that legitimately live on only one side, each with its reason.
# Kept deliberately tiny: every entry is a claim someone had to defend.
EXEMPT = {
    ("UpdateClipRequest", "clear_fields"): "control field — it names OTHER fields to clear",
}


def class_fields(tree, name):
    """Annotated field names of a class, in source order."""
    for n in ast.walk(tree):
        if isinstance(n, ast.ClassDef) and n.name == name:
            return [
                s.target.id
                for s in n.body
                if isinstance(s, ast.AnnAssign) and isinstance(s.target, ast.Name)
            ]
    return None


def changed_fields_tuple(src):
    """The literal tuple the PATCH response reports as changed."""
    m = re.search(
        r'"changed_fields":\s*\[\s*f\s+for\s+f\s+in\s*\((.*?)\)\s*if\s',
        src, re.DOTALL,
    )
    if not m:
        return None
    return set(re.findall(r'"([a-z_0-9]+)"', m.group(1)))


def handler_src(tree, src, fname):
    """Source text of one function, so matches can't leak in from elsewhere."""
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == fname:
            return ast.get_source_segment(src, n) or ""
    return ""


def applied_fields(handler):
    """Fields the PATCH handler actually writes onto the clip.

    A field counts as applied when the handler both READS `req.X` and
    WRITES `clip.X`. It deliberately does not require them in the same
    statement: most fields are validated into a local first
    (`clip.clip_mode = cm`, `clip.voiceover_anchor_image_node_id = anchor.id`),
    and demanding a bare `clip.X = req.X` produced fourteen false positives
    on the first run. A checker that cries wolf gets switched off, which is
    worse than no checker — so this is scoped to the handler body and
    calibrated to zero known-good failures.
    """
    written = set(re.findall(r'clip\.([a-z_0-9]+)\s*=', handler))
    read = set(re.findall(r'req\.([a-z_0-9]+)', handler))
    return written & read


def to_dict_keys(tree, cls):
    """String keys returned by a class's to_dict()."""
    for n in ast.walk(tree):
        if isinstance(n, ast.ClassDef) and n.name == cls:
            for f in n.body:
                if isinstance(f, ast.FunctionDef) and f.name == "to_dict":
                    return {
                        k.value
                        for d in ast.walk(f)
                        if isinstance(d, ast.Dict)
                        for k in d.keys
                        if isinstance(k, ast.Constant) and isinstance(k.value, str)
                    }
    return None


def sa_columns(tree, cls):
    """SQLAlchemy Column(...) attribute names on a model class."""
    out = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ClassDef) and n.name == cls:
            for s in n.body:
                if isinstance(s, ast.Assign) and isinstance(s.value, ast.Call):
                    fn = s.value.func
                    nm = getattr(fn, "id", None) or getattr(fn, "attr", None)
                    if nm == "Column":
                        for t in s.targets:
                            if isinstance(t, ast.Name):
                                out.add(t.id)
    return out


def main():
    problems, notes = [], []

    main_src = MAIN.read_text(encoding="utf-8")
    main_tree = ast.parse(main_src)
    index_src = INDEX.read_text(encoding="utf-8") if INDEX.exists() else ""
    imgp_tree = ast.parse(IMGP.read_text(encoding="utf-8"))

    # ---- CHECK 1 — the PATCH surface agrees with itself ------------------
    ucr = class_fields(main_tree, "UpdateClipRequest") or []
    patch_handler = handler_src(main_tree, main_src, "update_clip")
    if not patch_handler:
        notes.append("update_clip() not found — CHECK1 fell back to whole-file scope")
        patch_handler = main_src
    applied = applied_fields(patch_handler)
    reported = changed_fields_tuple(main_src)
    if reported is None:
        problems.append("CHECK1: could not locate the changed_fields tuple in main.py")
        reported = set()
    else:
        for f in ucr:
            if ("UpdateClipRequest", f) in EXEMPT:
                continue
            if f not in applied:
                problems.append(
                    "CHECK1 {0}: declared on UpdateClipRequest but never applied "
                    "(`clip.{0} = req.{0}` missing) — the API accepts it and drops it".format(f))
            elif f not in reported:
                problems.append(
                    "CHECK1 {0}: applied but MISSING from the changed_fields tuple — a "
                    "successful patch reports `changed_fields: []` and reads as a no-op "
                    "(this is v892.5)".format(f))
        for f in sorted(reported - set(ucr)):
            problems.append(
                "CHECK1 {0}: named in changed_fields but not a field of "
                "UpdateClipRequest — reports a change that can never happen".format(f))

    # ---- CHECK 2 — per-scene bindings reach the promote payload ----------
    # Derived: any DialogueLineInput field that main.py reads back out of the
    # submitted line dict is by construction expected to arrive from the
    # frontend. If index.html never names it, it always arrives as None.
    dli = class_fields(main_tree, "DialogueLineInput") or []
    read_back = set(re.findall(
        r"line(?:_data|_data_cp|_cp)?\.get\(\s*['\"]([a-z_0-9]+)['\"]", main_src))
    crossing = [f for f in dli if f in read_back]
    # The field must appear as an OBJECT KEY — start of line, then `name:`.
    # A looser `\bname\s*:` looked right and was worthless: `\s*` crosses
    # newlines, so it matched the colon of the ternary that READS the value
    # (`? promoteMeta.name\n : null`). The check passed on a copy with the key
    # deliberately deleted. Found only by negative-testing it.
    for f in crossing:
        if not re.search(r"^\s*" + re.escape(f) + r"\s*:", index_src, re.MULTILINE):
            problems.append(
                "CHECK2 {0}: DialogueLineInput declares it and main.py reads it back, but "
                "static/index.html never sends it — it arrives as None on every job "
                "(this is v892.2)".format(f))

    # ---- CHECK 3 — assignment columns survive to_dict --------------------
    cols = sa_columns(imgp_tree, "ImageSceneAssignment")
    keys = to_dict_keys(imgp_tree, "ImageSceneAssignment")
    if keys is None:
        notes.append("ImageSceneAssignment.to_dict() not found — CHECK3 skipped")
    else:
        # A `X_json` column is emitted under its de-suffixed name (`lines_json`
        # -> "lines"), so compare on both spellings. Bookkeeping columns are
        # not payload and are never expected downstream.
        BOOKKEEPING = {"id", "batch_id", "created_at", "updated_at"}
        for c in sorted(cols - keys):
            if c in BOOKKEEPING or re.sub(r"_json$", "", c) in keys:
                continue
            problems.append(
                "CHECK3 {0}: ImageSceneAssignment column is never emitted by to_dict() — "
                "nothing downstream can read it".format(c))

    # ---- report ---------------------------------------------------------
    print("=" * 74)
    print("FIELD PLUMBING  —  do the hand-maintained surfaces still agree?")
    print("=" * 74)
    print("UpdateClipRequest      : {0} fields | {1} applied | {2} reported".format(
        len(ucr), len(applied), len(reported)))
    print("DialogueLineInput      : {0} fields | {1} cross the frontend boundary".format(
        len(dli), len(crossing)))
    print("ImageSceneAssignment   : {0} columns | {1} to_dict keys".format(
        len(cols), len(keys) if keys is not None else "n/a"))
    for n in notes:
        print("  note: {0}".format(n))
    print()

    if problems:
        print("FAIL — {0} field(s) wired at some surfaces and not others:".format(len(problems)))
        for p in problems:
            print("   " + p)
        print()
        print("Every one of these is SILENT at runtime. Fix the missing surface, or add")
        print("an EXEMPT entry with the reason if the field is genuinely one-sided.")
        return 1

    print("PASS — every field reaches every surface it has to cross.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
