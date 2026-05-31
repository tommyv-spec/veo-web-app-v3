"""Tests for instagram_match.score + best_matches."""
import importlib.util
import pathlib


def _load():
    spec = importlib.util.spec_from_file_location(
        "instagram_match", pathlib.Path(__file__).parent / "instagram_match.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_score_identical_strings_returns_one():
    m = _load()
    s = m.score("his bedroom looked like this thirty days ago", "his bedroom looked like this thirty days ago")
    assert s == 1.0 or s > 0.99


def test_score_zero_for_unrelated():
    m = _load()
    s = m.score("the cat sat on the mat", "completely different content here xyz")
    assert s < 0.5


def test_score_boost_for_3gram_match():
    m = _load()
    base = m.score("his bedroom looked terrible thirty days ago",
                   "his bedroom looked entirely different now")
    # Both have the 3-gram "his bedroom looked" so boost should kick in.
    assert base >= 0.4


def test_score_normalization_lowercase_punct():
    m = _load()
    a = "His Bedroom, Looked Like This!!!"
    b = "his bedroom looked like this"
    assert m.score(a, b) > 0.95


def test_best_matches_sorted_descending():
    m = _load()
    ig = type("V", (), {"transcription": "looked like this thirty days ago", "id": 1})()
    class J:
        def __init__(self, id_, dlg):
            self.id = id_
            self._dlg = dlg
    jobs = [J("a", "totally unrelated text"), J("b", "his bedroom looked like this thirty days ago"), J("c", "looked like this once")]
    result = m.best_matches(ig, jobs, full_dialogue=lambda j: j._dlg, k=5, min_score=0.0)
    assert result[0]["job_id"] == "b"
    assert result[0]["score"] >= result[1]["score"] >= result[2]["score"]


def test_best_matches_threshold_filters():
    m = _load()
    ig = type("V", (), {"transcription": "needle in haystack", "id": 1})()
    class J:
        def __init__(self, id_, dlg):
            self.id = id_
            self._dlg = dlg
    jobs = [J("a", "completely different"), J("b", "needle in haystack exactly")]
    result = m.best_matches(ig, jobs, full_dialogue=lambda j: j._dlg, k=5, min_score=0.7)
    # Only the close match should make it past the threshold.
    assert len(result) == 1 and result[0]["job_id"] == "b"


def test_score_handles_empty_inputs():
    m = _load()
    assert m.score("", "") == 0.0
    assert m.score("anything", "") == 0.0
    assert m.score("", "anything") == 0.0
