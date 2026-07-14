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


# ---- v822.6: idf_power (rare-term weighting) + BM25 -----------------------
def test_idf_power_suppresses_generic_attractor():
    """A long, generic candidate that shares only COMMON words must not beat
    the short candidate that shares the query's DISTINCTIVE recipe word."""
    m = _load()
    cands = [
        ("specific", "ginger lemon honey"),
        ("generic", "soldier blood flow morning soldier blood flow saffron soldier blood flow drive soldier"),
    ]
    q = "ginger lemon honey soldier blood flow"
    r2 = m.rank_tfidf(q, cands, idf_power=2.0)
    assert r2[0]["job_id"] == "specific"


def test_idf_power_default_is_backward_compatible():
    m = _load()
    cands = [("a", "one two three"), ("b", "four five six")]
    base = m.rank_tfidf("one two three", cands)
    p1 = m.rank_tfidf("one two three", cands, idf_power=1.0)
    assert base[0]["job_id"] == p1[0]["job_id"] == "a"


def test_bm25_ranks_and_normalizes_to_unit_top():
    m = _load()
    cands = [("a", "ginger lemon honey drink"), ("b", "totally different words here")]
    r = m.rank_bm25("ginger lemon honey", cands)
    assert r[0]["job_id"] == "a"
    assert r[0]["score"] == 1.0  # min-max normalised: top is 1.0
    assert r[1]["score"] < r[0]["score"]


def test_bm25_empty():
    m = _load()
    assert m.rank_bm25("", [("a", "x")]) == []
    assert m.rank_bm25("x", []) == []


# ---- v852: spoken-text reconstruction (Prompt B + final cut) --------------

def _clip(**kw):
    """A Clip row as the bulk builder hands it to the pure rules."""
    base = {
        "id": 1, "clip_index": 0, "clip_role": None, "paired_clip_id": None,
        "dialogue_text": "", "dialogue_text_b": None,
        "rendered_prompt_variant": "A", "voiceover_line": None,
        "approval_status": "approved",
    }
    base.update(kw)
    return base


def test_spoken_line_variant_a_uses_dialogue_text():
    m = _load()
    c = _clip(dialogue_text="your soldier wont wake up", dialogue_text_b="a reworded line")
    assert m.spoken_line(c) == "your soldier wont wake up"


def test_spoken_line_variant_b_uses_reworded_line():
    m = _load()
    c = _clip(dialogue_text="the banned wording",
              dialogue_text_b="the reworded line that was actually said",
              rendered_prompt_variant="B")
    assert m.spoken_line(c) == "the reworded line that was actually said"


def test_spoken_line_variant_b_without_b_text_falls_back():
    m = _load()
    c = _clip(dialogue_text="original line", dialogue_text_b=None,
              rendered_prompt_variant="B")
    assert m.spoken_line(c) == "original line"


def test_reconstruct_uses_audio_twin_not_stale_voiceover_line():
    """The audio_pair renders the speech. When IT fell back to Prompt B, the
    visual twin's voiceover_line is stale and must NOT be used."""
    m = _load()
    clips = [
        _clip(id=10, clip_index=0, clip_role="visual_pair",
              dialogue_text="", voiceover_line="the stale original line"),
        _clip(id=11, clip_index=100000, clip_role="audio_pair", paired_clip_id=10,
              dialogue_text="the stale original line",
              dialogue_text_b="the reworded line actually spoken",
              rendered_prompt_variant="B"),
    ]
    assert m.reconstruct_dialogue(clips) == "the reworded line actually spoken"


def test_reconstruct_visual_pair_without_twin_falls_back_to_voiceover_line():
    m = _load()
    clips = [_clip(id=10, clip_role="visual_pair", dialogue_text="",
                   voiceover_line="spoken over the b-roll")]
    assert m.reconstruct_dialogue(clips) == "spoken over the b-roll"


def test_reconstruct_drops_clips_not_in_the_final_cut():
    m = _load()
    clips = [
        _clip(id=1, clip_index=0, dialogue_text="kept line", approval_status="approved"),
        _clip(id=2, clip_index=1, dialogue_text="rejected line", approval_status="rejected"),
        _clip(id=3, clip_index=2, dialogue_text="pending line", approval_status="pending_review"),
    ]
    assert m.reconstruct_dialogue(clips) == "kept line"


def test_reconstruct_never_blanks_a_job_with_no_approved_clips():
    """A legacy job with nothing marked approved must still produce text —
    blank text would silently drop it from the candidate pool entirely."""
    m = _load()
    clips = [
        _clip(id=1, clip_index=0, dialogue_text="first line", approval_status="pending_review"),
        _clip(id=2, clip_index=1, dialogue_text="second line", approval_status="pending_review"),
    ]
    assert m.reconstruct_dialogue(clips) == "first line second line"


def test_reconstruct_orders_by_clip_index():
    m = _load()
    clips = [
        _clip(id=2, clip_index=1, dialogue_text="second"),
        _clip(id=1, clip_index=0, dialogue_text="first"),
    ]
    assert m.reconstruct_dialogue(clips) == "first second"


# ---- v852: time constraint + confidence verdict ---------------------------
import datetime as _dt


def test_job_created_after_the_reel_was_posted_is_impossible():
    m = _load()
    posted = _dt.datetime(2026, 6, 1, 12, 0, 0)
    created_after = _dt.datetime(2026, 6, 10, 12, 0, 0)   # 9 days AFTER the post
    assert m.job_predates_post(created_after, posted) is False


def test_job_created_before_the_reel_is_eligible():
    m = _load()
    posted = _dt.datetime(2026, 6, 10, 12, 0, 0)
    created_before = _dt.datetime(2026, 6, 1, 12, 0, 0)
    assert m.job_predates_post(created_before, posted) is True


def test_job_created_just_after_post_survives_on_clock_skew_slack():
    m = _load()
    posted = _dt.datetime(2026, 6, 10, 12, 0, 0)
    created = _dt.datetime(2026, 6, 10, 20, 0, 0)  # 8h later — within 1-day slack
    assert m.job_predates_post(created, posted) is True


def test_unknown_timestamps_never_exclude():
    m = _load()
    assert m.job_predates_post(None, _dt.datetime(2026, 6, 1)) is True
    assert m.job_predates_post(_dt.datetime(2026, 6, 1), None) is True


def test_verdict_confident_when_top_is_high_and_clear():
    m = _load()
    ranked = [{"job_id": "a", "score": 0.80}, {"job_id": "b", "score": 0.40}]
    v = m.match_verdict(ranked, high=0.50, margin=0.12)
    assert v["verdict"] == "confident"


def test_verdict_ambiguous_when_twins_are_neck_and_neck():
    m = _load()
    ranked = [{"job_id": "a", "score": 0.80}, {"job_id": "b", "score": 0.78}]
    v = m.match_verdict(ranked, high=0.50, margin=0.12)
    assert v["verdict"] == "ambiguous"


def test_verdict_weak_when_nothing_scores_well():
    m = _load()
    ranked = [{"job_id": "a", "score": 0.20}, {"job_id": "b", "score": 0.05}]
    v = m.match_verdict(ranked, high=0.50, margin=0.12)
    assert v["verdict"] == "weak"


def test_verdict_does_not_round_a_hair_thin_gap_into_confidence():
    """A gap of 0.11996 must stay AMBIGUOUS under a 0.12 margin — rounding it
    to 0.12 first would report a coin-flip between twins as a certainty."""
    m = _load()
    ranked = [{"job_id": "a", "score": 0.81996}, {"job_id": "b", "score": 0.70000}]
    v = m.match_verdict(ranked, high=0.50, margin=0.12)
    assert v["verdict"] == "ambiguous"


def test_verdict_none_on_empty_ranking():
    m = _load()
    assert m.match_verdict([], high=0.5, margin=0.12)["verdict"] == "none"
