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


# ---- v822.4: TF-IDF cosine + margin gate (local matcher) ------------------
def test_rank_tfidf_ranks_correct_doc_first():
    m = _load()
    cands = [
        ("a", "comment saffron below your soldier blood flow every morning"),
        ("b", "comment saffron below your soldier blood flow ginger turmeric recipe"),
        ("c", "comment saffron below your soldier blood flow beetroot pour timing"),
    ]
    # query = doc c with a distinctive word; despite shared boilerplate it wins
    ranked = m.rank_tfidf("your soldier blood flow beetroot pour timing morning", cands)
    assert ranked[0]["job_id"] == "c"


def test_rank_tfidf_boilerplate_does_not_dominate():
    m = _load()
    # Two docs share ALL the boilerplate; only the distinctive tail differs.
    cands = [
        ("x", "your soldier blood flow saffron ginger"),
        ("y", "your soldier blood flow saffron beetroot"),
    ]
    ranked = m.rank_tfidf("your soldier blood flow saffron ginger", cands)
    # ginger query must rank x above y (distinctive term drives it).
    assert ranked[0]["job_id"] == "x"
    assert ranked[0]["score"] > ranked[1]["score"]


def test_rank_tfidf_empty():
    m = _load()
    assert m.rank_tfidf("", [("a", "text")]) == []
    assert m.rank_tfidf("text", []) == []


def test_auto_pick_requires_high_and_margin():
    m = _load()
    ranked = [{"job_id": "a", "score": 0.80}, {"job_id": "b", "score": 0.50}]
    assert m.auto_pick(ranked, high=0.5, margin=0.12) == "a"


def test_auto_pick_defers_on_small_margin():
    m = _load()
    # near-duplicate twins: high score but nearly tied -> MANUAL (None)
    ranked = [{"job_id": "a", "score": 0.96}, {"job_id": "b", "score": 0.95}]
    assert m.auto_pick(ranked, high=0.5, margin=0.12) is None


def test_auto_pick_defers_on_low_top():
    m = _load()
    ranked = [{"job_id": "a", "score": 0.30}, {"job_id": "b", "score": 0.05}]
    assert m.auto_pick(ranked, high=0.5, margin=0.12) is None


def test_auto_pick_single_candidate_needs_high():
    m = _load()
    assert m.auto_pick([{"job_id": "a", "score": 0.60}], high=0.5, margin=0.12) == "a"
    assert m.auto_pick([{"job_id": "a", "score": 0.40}], high=0.5, margin=0.12) is None
    assert m.auto_pick([], high=0.5, margin=0.12) is None
