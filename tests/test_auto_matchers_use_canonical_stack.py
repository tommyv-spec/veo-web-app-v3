"""The IG + drive AUTO-matchers must use the canonical matching stack.

Both auto-publish. Both used to run the char-level `best_matches` over a
private per-job `_full_dialogue` (old text rule: COALESCE(voiceover_line,
dialogue_text), audio_pair excluded in SQL). That matcher scored near-duplicate
scripts at ~1.000 against the WRONG twin and confidently published it.

Canonical stack (already feeding the suggestions endpoint + local watcher):
  local_transcribe._bulk_dialogue_map  -> Prompt-B aware, final-cut aware, 1 query
  instagram_match.rank_tfidf           -> TF-IDF cosine (idf_power=2)
  instagram_match.auto_pick            -> HIGH + MARGIN gate (ambiguous -> manual)

Source-grep asserts (same pattern as tests/test_local_watch_never_miss.py):
this codebase has been bitten by regressions py_compile does not catch.
"""
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_CODE = os.path.dirname(_HERE)
_IG = os.path.join(_CODE, "instagram_transcribe.py")
_DRIVE = os.path.join(_CODE, "drive_transcribe.py")

_AUTO_MATCHERS = {"instagram_transcribe.py": _IG, "drive_transcribe.py": _DRIVE}


def _src(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def test_auto_matchers_have_no_private_full_dialogue():
    """The per-job N+1 dialogue builder with the OLD text rule is deleted."""
    for name, path in _AUTO_MATCHERS.items():
        src = _src(path)
        assert "def _full_dialogue(" not in src, f"{name} still defines _full_dialogue"


def test_auto_matchers_do_not_call_best_matches():
    """The char-level matcher published the wrong twin. It must be gone."""
    for name, path in _AUTO_MATCHERS.items():
        src = _src(path)
        assert "best_matches(" not in src, f"{name} still calls best_matches"


def test_auto_matchers_use_the_canonical_dialogue_builder():
    for name, path in _AUTO_MATCHERS.items():
        src = _src(path)
        assert "_bulk_dialogue_map" in src, f"{name} does not use _bulk_dialogue_map"


def test_auto_matchers_use_the_margin_gate():
    for name, path in _AUTO_MATCHERS.items():
        src = _src(path)
        assert "auto_pick(" in src, f"{name} does not use auto_pick"
        assert "rank_tfidf(" in src, f"{name} does not use rank_tfidf"
        assert "_MATCH_HIGH" in src, f"{name} does not use _MATCH_HIGH"
        assert "_MATCH_MARGIN" in src, f"{name} does not use _MATCH_MARGIN"


def test_auto_matchers_drop_the_char_level_threshold():
    """0.85/0.70 was a CHAR-level score — meaningless against TF-IDF cosine."""
    for name, path in _AUTO_MATCHERS.items():
        src = _src(path)
        assert "_AUTO_MATCH_THRESHOLD" not in src, f"{name} still carries the char-level threshold"


def test_auto_matchers_log_the_decision():
    """The operator calibrates the gate from the Render log — the decision line
    must carry the scores, the margin, the verdict and the pool size."""
    for marker, path in (("[ig-auto]", _IG), ("[drive-auto]", _DRIVE)):
        src = _src(path)
        assert marker in src, f"{path} missing the {marker} diagnostic prefix"
        for field in ("s1=", "s2=", "margin=", "decision=", "pool="):
            assert field in src, f"{path} decision log missing {field}"


def test_no_import_cycle_local_transcribe_back_into_the_auto_matchers():
    """local_transcribe imports instagram_transcribe (the cached Whisper model),
    so instagram_transcribe must NOT import local_transcribe at module scope."""
    lt = _src(os.path.join(_CODE, "local_transcribe.py"))
    assert "import drive_transcribe" not in lt
    assert "from drive_transcribe" not in lt
    assert "from instagram_transcribe import _model" in lt  # the one allowed edge
    ig = _src(_IG)
    for line in ig.splitlines():
        if line.startswith(("import ", "from ")):
            assert "local_transcribe" not in line, (
                "module-scope import of local_transcribe in instagram_transcribe "
                "would close the cycle — import it inside the function"
            )
