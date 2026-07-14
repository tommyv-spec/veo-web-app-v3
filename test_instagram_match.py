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
        "approval_status": "approved", "status": "completed",
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


def test_reconstruct_drops_rejected_clips_not_in_the_final_cut():
    m = _load()
    clips = [
        _clip(id=1, clip_index=0, dialogue_text="kept line", approval_status="approved"),
        _clip(id=2, clip_index=1, dialogue_text="rejected line", approval_status="rejected"),
    ]
    assert m.reconstruct_dialogue(clips) == "kept line"


def test_reconstruct_keeps_a_pending_clip_that_is_in_the_exported_cut():
    """A post-export redo flips an approved clip back to pending_review, but the
    mp4 already posted still SPEAKS that line. Dropping it would delete a whole
    line's rare terms from the job's text while the reel's transcript still has
    them — worse than keeping a slightly stale line."""
    m = _load()
    clips = [
        _clip(id=1, clip_index=0, dialogue_text="first line", approval_status="approved"),
        _clip(id=2, clip_index=1, dialogue_text="redone line", approval_status="pending_review"),
    ]
    assert m.reconstruct_dialogue(clips) == "first line redone line"


def test_reconstruct_still_drops_an_explicitly_rejected_clip():
    m = _load()
    clips = [
        _clip(id=1, clip_index=0, dialogue_text="kept", approval_status="approved"),
        _clip(id=2, clip_index=1, dialogue_text="rejected", approval_status="rejected"),
    ]
    assert m.reconstruct_dialogue(clips) == "kept"


def test_reconstruct_drops_clips_that_never_rendered():
    """failed / skipped / still-generating clips are not in the export."""
    m = _load()
    clips = [
        _clip(id=1, clip_index=0, dialogue_text="rendered", status="completed"),
        _clip(id=2, clip_index=1, dialogue_text="failed one", status="failed"),
        _clip(id=3, clip_index=2, dialogue_text="skipped one", status="skipped"),
    ]
    assert m.reconstruct_dialogue(clips) == "rendered"


def test_reconstruct_honours_a_custom_lineup_over_approval():
    """A lineup export selects by clip id and ignores approval entirely
    (main.py:9232-9241), so the lineup IS the final cut."""
    m = _load()
    clips = [
        _clip(id=1, clip_index=0, dialogue_text="in the lineup", approval_status="pending_review"),
        _clip(id=2, clip_index=1, dialogue_text="approved but cut", approval_status="approved"),
    ]
    assert m.reconstruct_dialogue(clips, lineup_ids=[1]) == "in the lineup"


def test_reconstruct_lineup_still_never_blanks():
    m = _load()
    clips = [_clip(id=1, clip_index=0, dialogue_text="only line")]
    # lineup names a clip id that does not exist -> selection empty -> fall back
    assert m.reconstruct_dialogue(clips, lineup_ids=[999]) == "only line"


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


# ============================================================================
# v855 — evidence_pick: the MEDIA decides, not the words.
#
# Every envelope below is a real fingerprint (encode_fingerprint over a
# synthetic loudness envelope), so these tests exercise the same
# envelope_similarity path production runs.
# ============================================================================
import math as _math

import audio_fingerprint as _afp


def _fp(seed, n=120, phase=0.0):
    """A deterministic, distinctive envelope -> a real base64 fingerprint."""
    env = [
        abs(_math.sin((i + phase) * (0.07 + 0.013 * seed)) + 0.35 * _math.cos((i + phase) * 0.31 * (seed + 1)))
        for i in range(n)
    ]
    norm = _math.sqrt(sum(x * x for x in env)) or 1.0
    return _afp.encode_fingerprint([x / norm for x in env])


def _cand(job_id, dur, fp=None):
    return {"job_id": job_id, "export_duration_s": dur, "export_audio_fp": fp}


def test_evidence_duration_decisive():
    m = _load()
    r = m.evidence_pick(30.0, None, [_cand("a", 30.1), _cand("b", 34.0)])
    assert r["job_id"] == "a"
    assert r["source"] == "duration"
    assert r["conflict"] is False
    assert abs(r["dur_delta"] - 0.1) < 1e-6


def test_evidence_duration_too_close_to_call():
    """Two candidates equally close -> the runner-up separation gate fails."""
    m = _load()
    r = m.evidence_pick(30.0, None, [_cand("a", 30.1), _cand("b", 29.9)])
    assert r["job_id"] is None
    assert r["source"] is None
    assert r["conflict"] is False


def test_evidence_duration_abstains_when_nothing_is_close_enough():
    m = _load()
    r = m.evidence_pick(30.0, None, [_cand("a", 45.0), _cand("b", 60.0)])
    assert r["job_id"] is None


def test_evidence_waveform_decisive():
    m = _load()
    reel = _fp(1)
    r = m.evidence_pick(30.0, reel, [_cand("a", 30.05, reel), _cand("b", 30.1, _fp(9))])
    assert r["job_id"] == "a"
    assert "waveform" in (r["source"] or "")
    assert r["similarity"] >= m.WAVE_MIN_SIM


def test_evidence_waveform_abstains_on_low_similarity():
    """A TRUE match can score as low as 0.53 — so a low score means ABSTAIN,
    never 'the other one'. Abstaining is the correct behavior."""
    m = _load()
    r = m.evidence_pick(30.0, _fp(1), [_cand("a", 30.05, _fp(7)), _cand("b", 30.1, _fp(9))])
    assert r["similarity"] is None or r["similarity"] < m.WAVE_MIN_SIM
    assert r["job_id"] is None
    assert r["conflict"] is False


def test_evidence_waveform_breaks_an_identical_duration_tie():
    """THE case that motivated all of this: two builds, same script, exports the
    SAME length to the last bit. Duration cannot separate them; the take does."""
    m = _load()
    reel = _fp(3)
    cands = [_cand("twin_a", 46.02000045776367, _fp(11)),
             _cand("twin_b", 46.02000045776367, reel)]
    dur_only = m.evidence_pick(46.0, None, cands)
    assert dur_only["job_id"] is None          # duration is helpless here
    r = m.evidence_pick(46.0, reel, cands)
    assert r["job_id"] == "twin_b"
    assert r["source"] == "waveform"
    assert r["similarity"] >= m.WAVE_MIN_SIM


def test_evidence_both_agree_reports_both_sources():
    m = _load()
    reel = _fp(2)
    r = m.evidence_pick(30.0, reel, [_cand("a", 30.05, reel), _cand("b", 31.2, _fp(8))])
    assert r["job_id"] == "a"
    assert r["source"] == "waveform+duration"


def test_evidence_conflict_yields_no_pick():
    """Duration says A, the waveform says B. Never seen in validation — but a
    disagreement means we do not understand the data, and guessing is exactly
    the failure this exists to fix."""
    m = _load()
    reel = _fp(4)
    # A is nearest by duration (0.0 vs 1.2 -> decisive), but B's audio IS the reel.
    r = m.evidence_pick(30.0, reel, [_cand("a", 30.0, _fp(12)), _cand("b", 31.2, reel)])
    assert r["conflict"] is True
    assert r["job_id"] is None
    assert r["source"] is None


def test_evidence_missing_data_never_raises_and_never_guesses():
    m = _load()
    assert m.evidence_pick(None, None, [])["job_id"] is None
    assert m.evidence_pick(None, None, [_cand("a", 30.0, _fp(1))])["job_id"] is None
    assert m.evidence_pick(30.0, None, [_cand("a", None, None)])["job_id"] is None
    assert m.evidence_pick(30.0, _fp(1), [_cand("a", None, _fp(1))])["job_id"] is None
    assert m.evidence_pick(30.0, "", [_cand("a", 30.0, "")])["job_id"] == "a"  # duration still works


def test_evidence_waveform_prefilters_far_durations():
    """Anything further than 1.5s away cannot be the same render — and the
    comparison is expensive, so it is never even attempted."""
    m = _load()
    reel = _fp(5)
    r = m.evidence_pick(30.0, reel, [_cand("far", 40.0, reel), _cand("near", 30.2, _fp(13))])
    assert r["job_id"] != "far"


def test_evidence_caps_the_number_of_waveform_comparisons():
    """envelope_similarity is O(lag x frames) pure Python. The diag endpoint
    502'd once from doing too many in one request; the cap is the fix."""
    m = _load()
    cands = [_cand(f"j{i}", 30.0 + i * 0.01, _fp(i + 20)) for i in range(40)]
    r = m.evidence_pick(30.0, _fp(5), cands)   # must return, not hang
    assert r["conflict"] is False
    assert m.WAVE_MAX_COMPARISONS <= 12


def test_recency_window_keeps_recent_jobs_and_drops_old_ones():
    m = _load()
    posted = _dt.datetime(2026, 7, 1)
    assert m.within_recency_window(_dt.datetime(2026, 6, 25), posted) is True
    assert m.within_recency_window(_dt.datetime(2026, 5, 1), posted) is False   # 61d old
    assert m.within_recency_window(_dt.datetime(2026, 7, 20), posted) is False  # created after
    assert m.within_recency_window(None, posted) is True                        # unknown never excludes
    assert m.within_recency_window(_dt.datetime(2026, 5, 1), None) is True


# ============================================================================
# v856 — THE FILENAME IS THE ANSWER.
#
# The platform mints the export filename itself, so it can stamp the job id
# into it. When that stamp is present the match is a LOOKUP, not a guess — it
# beats text, duration and waveform, none of which can separate two renders of
# the same script.
# ============================================================================

def test_job_id_from_filename_reads_the_stamp():
    m = _load()
    assert m.job_id_from_filename(
        "final_export_6e52de72_20260714_120000_a1b2c3.mp4") == "6e52de72"


def test_job_id_from_filename_handles_the_broll_variant():
    m = _load()
    assert m.job_id_from_filename(
        "final_broll_6e52de72_20260714_120000_a1b2c3.mp4") == "6e52de72"


def test_job_id_from_filename_is_case_insensitive_and_normalizes_down():
    """A filesystem (or the operator) may hand the name back upper-cased. The
    job ids are lowercase hex, and the caller feeds this straight into a
    case-sensitive SQL LIKE — so normalize here, at the source."""
    m = _load()
    assert m.job_id_from_filename(
        "FINAL_EXPORT_6E52DE72_20260714_120000_A1B2C3.MP4") == "6e52de72"


def test_job_id_from_filename_ignores_a_renamed_file():
    m = _load()
    assert m.job_id_from_filename("my saffron reel FINAL v3.mp4") is None
    assert m.job_id_from_filename("6e52de72.mp4") is None


def test_job_id_from_filename_ignores_the_id_in_the_wrong_place():
    """The stamp only counts where the platform puts it: right after the
    prefix. An id anywhere else is a coincidence, not a claim."""
    m = _load()
    assert m.job_id_from_filename(
        "6e52de72_final_export_20260714_120000_a1b2c3.mp4") is None
    assert m.job_id_from_filename(
        "final_export_20260714_120000_6e52de72.mp4") is None


def test_job_id_from_filename_does_not_mistake_a_LEGACY_name_for_a_stamp():
    """THE regression this whole feature can die on.

    Legacy shape:  final_export_<YYYYMMDD>_<HHMMSS>_<hash>.mp4
    Stamped shape: final_export_<job8>_<YYYYMMDD>_<HHMMSS>_<hash>.mp4

    A date like `20260714` is EIGHT VALID HEX CHARS sitting in exactly the slot
    the job id now occupies. A regex that only asks for [0-9a-f]{8} after the
    prefix therefore reads every legacy export as job id "20260714" — which
    resolves to zero jobs at best, and to a WRONG job the day a real job id
    starts with those digits. Legacy names must return None and fall through to
    the evidence path, so the trailing timestamp is part of the pattern.
    """
    m = _load()
    assert m.job_id_from_filename("final_export_20260714_120000_a1b2c3.mp4") is None
    assert m.job_id_from_filename("final_broll_20260714_120000_a1b2c3.mp4") is None
    # ...and the all-decimal job id (a real possibility: 10^8/16^8 of uuids)
    # still reads, because the timestamp that follows it is what proves the slot.
    assert m.job_id_from_filename(
        "final_export_12345678_20260714_120000_a1b2c3.mp4") == "12345678"


def test_job_id_from_filename_needs_only_the_date_not_the_whole_timestamp():
    """The stamp is proven by the DATE that follows it, not the full timestamp.

    Legacy is <8>_<6>_<6> and stamped is <8>_<8>_<6>_<6>, so 8 digits in the
    second slot is already enough to tell them apart — and only requiring the
    date keeps the reader working on any shortened variant of the name.
    """
    m = _load()
    assert m.job_id_from_filename(
        "final_export_6e52de72_20260714_a1b2c3.mp4") == "6e52de72"


def test_job_id_from_filename_survives_junk_input():
    m = _load()
    assert m.job_id_from_filename(None) is None
    assert m.job_id_from_filename("") is None
    assert m.job_id_from_filename(123) is None


def test_job_id_from_filename_reads_through_a_voice_cloned_wrapper():
    """voice_cloned_<voice>_<orig>.mp4 wraps the export name — and it IS that
    job's audio, so the stamp inside it still identifies the job correctly."""
    m = _load()
    assert m.job_id_from_filename(
        "voice_cloned_nuri_final_export_6e52de72_20260714_120000_a1b2c3.mp4"
    ) == "6e52de72"


def test_job_id_from_filename_ignores_a_non_hex_segment():
    m = _load()
    assert m.job_id_from_filename(
        "final_export_zzzzzzzz_20260714_120000_a1b2c3.mp4") is None


# --- the minting side: one definition, so writer and reader cannot drift -----

def test_export_job_segment_is_the_first_8_hex_chars():
    m = _load()
    assert m.export_job_segment("6e52de72-9c1f-4b0a-8f3e-1d2c3b4a5f60") == "6e52de72"


def test_export_job_segment_refuses_a_non_uuid_job_id():
    """A non-hex id cannot be stamped without breaking the reader, so it is not
    stamped at all — the name falls back to the legacy shape and the watcher
    falls back to evidence. Degrade, never corrupt."""
    m = _load()
    assert m.export_job_segment("not-a-uuid-at-all") is None
    assert m.export_job_segment("") is None
    assert m.export_job_segment(None) is None


def test_minted_name_round_trips_through_the_reader():
    """The contract, end to end: what main.py writes is what the watcher reads."""
    m = _load()
    job_id = "6e52de72-9c1f-4b0a-8f3e-1d2c3b4a5f60"
    seg = m.export_job_segment(job_id)
    name = f"final_export_{seg}_20260714_120000_a1b2c3.mp4"
    assert name.startswith(("final_export_", "final_broll_", "export_"))  # prefix detectors
    assert m.job_id_from_filename(name) == seg
    assert job_id.startswith(m.job_id_from_filename(name))


# ============================================================================
# v857 — ONE JOB, ONE VIDEO (except a repost).
#
# claim_strength ranks two videos claiming the SAME job, so the stronger one
# takes it instead of the first writer keeping it. is_same_video is the one
# legitimate exception: the operator posting a single export twice.
# ============================================================================

def _ev(source=None, similarity=None, dur_delta=None, conflict=False, job_id="j"):
    """An evidence_pick-shaped dict."""
    return {"job_id": job_id if source else None, "source": source,
            "similarity": similarity, "dur_delta": dur_delta, "conflict": conflict}


def test_claim_strength_no_evidence_is_zero():
    m = _load()
    assert m.claim_strength(_ev()) == 0.0
    assert m.claim_strength({}) == 0.0
    assert m.claim_strength(None) == 0.0


def test_claim_strength_orders_the_three_kinds_of_evidence():
    """waveform+duration > waveform alone > duration alone — ALWAYS, including
    the worst waveform against the best duration. A length match is 'the same
    length'; a waveform match is 'the same performance'."""
    m = _load()
    best_dur = m.claim_strength(_ev("duration", dur_delta=0.0))
    worst_wave = m.claim_strength(_ev("waveform", similarity=m.WAVE_MIN_SIM))
    best_wave = m.claim_strength(_ev("waveform", similarity=1.0))
    worst_both = m.claim_strength(
        _ev("waveform+duration", similarity=m.WAVE_MIN_SIM, dur_delta=m.DUR_TOLERANCE_S))
    assert 0.0 < best_dur < worst_wave <= best_wave < worst_both


def test_claim_strength_a_weak_waveform_still_beats_a_strong_duration():
    """The exact inversion the gap-free score exists to prevent: a 0.90 waveform
    must NOT lose to a 0.9s duration (nor to a 0.0s one)."""
    m = _load()
    wave_090 = m.claim_strength(_ev("waveform", similarity=0.90))
    dur_09s = m.claim_strength(_ev("duration", dur_delta=0.9))
    dur_0s = m.claim_strength(_ev("duration", dur_delta=0.0))
    assert wave_090 > dur_09s
    assert wave_090 > dur_0s


def test_claim_strength_within_duration_a_tighter_delta_is_stronger():
    """0.005s beats 0.632s — the production numbers."""
    m = _load()
    tight = m.claim_strength(_ev("duration", dur_delta=0.005))
    loose = m.claim_strength(_ev("duration", dur_delta=0.632))
    assert tight > loose > 0.0


def test_claim_strength_within_waveform_a_higher_similarity_is_stronger():
    m = _load()
    assert m.claim_strength(_ev("waveform", similarity=0.98)) > \
           m.claim_strength(_ev("waveform", similarity=0.91))


def test_claim_strength_is_continuous_in_each_signal():
    """No cliffs INSIDE a kind of evidence: nudging the similarity or the delta
    a hair may not move the score by a landslide."""
    m = _load()
    a = m.claim_strength(_ev("waveform", similarity=0.940))
    b = m.claim_strength(_ev("waveform", similarity=0.941))
    assert 0 < (b - a) < 0.05
    c = m.claim_strength(_ev("duration", dur_delta=0.500))
    d = m.claim_strength(_ev("duration", dur_delta=0.501))
    assert 0 < (c - d) < 0.05


def test_claim_strength_ignores_a_similarity_the_waveform_did_not_decide_on():
    """evidence_pick REPORTS a similarity when the DURATION decided (source
    'duration') — it is informational, it did not pass the gate. Scoring it
    would promote a duration-only claim into the waveform band."""
    m = _load()
    reported = m.claim_strength(_ev("duration", similarity=0.99, dur_delta=0.4))
    assert reported < m.claim_strength(_ev("waveform", similarity=m.WAVE_MIN_SIM))


def test_claim_strength_never_raises_on_junk():
    m = _load()
    assert m.claim_strength({"source": "waveform", "similarity": None}) >= 0.0
    assert m.claim_strength({"source": "duration", "dur_delta": "abc"}) == 0.0
    assert m.claim_strength({"source": 123}) == 0.0


# ---- is_same_video: the repost, the one legitimate double claim -------------

def test_is_same_video_true_for_a_repost():
    """Operator-confirmed: the same export posted twice — identical duration to
    the bit, near-identical waveform."""
    m = _load()
    fp = _fp(3)
    assert m.is_same_video(fp, fp, 46.02, 46.02) is True


def test_is_same_video_false_for_two_different_performances():
    m = _load()
    assert m.is_same_video(_fp(3), _fp(9), 46.02, 46.02) is False


def test_is_same_video_false_when_the_durations_disagree():
    """Same-ish audio but a different runtime is a different cut, not a repost."""
    m = _load()
    fp = _fp(3)
    assert m.is_same_video(fp, fp, 46.02, 47.50) is False


def test_is_same_video_false_when_a_fingerprint_is_missing():
    """Cannot CONFIRM a repost -> treat as two different videos -> exclusivity
    applies. Absence of evidence is never evidence."""
    m = _load()
    assert m.is_same_video(None, _fp(3), 46.02, 46.02) is False
    assert m.is_same_video(_fp(3), "", 46.02, 46.02) is False
    assert m.is_same_video("", "", 46.02, 46.02) is False


def test_is_same_video_false_when_a_duration_is_missing():
    m = _load()
    fp = _fp(3)
    assert m.is_same_video(fp, fp, None, 46.02) is False
    assert m.is_same_video(fp, fp, 46.02, None) is False


def test_is_same_video_never_raises_on_a_malformed_fingerprint():
    m = _load()
    assert m.is_same_video("!!not base64!!", _fp(3), 46.0, 46.0) is False


# ---- resolve_claim: link | steal | refuse -----------------------------------

def test_resolve_claim_links_when_there_is_no_incumbent_strength_but_a_repost():
    m = _load()
    assert m.resolve_claim(0.0, 9.9, True) == "link"


def test_resolve_claim_steals_when_the_challenger_is_clearly_stronger():
    m = _load()
    strong = m.claim_strength(_ev("waveform+duration", similarity=0.947, dur_delta=0.005))
    weak = m.claim_strength(_ev("duration", dur_delta=0.632))
    assert m.resolve_claim(strong, weak, False) == "steal"


def test_resolve_claim_refuses_when_the_challenger_is_weaker():
    m = _load()
    strong = m.claim_strength(_ev("waveform+duration", similarity=0.947, dur_delta=0.005))
    weak = m.claim_strength(_ev("duration", dur_delta=0.632))
    assert m.resolve_claim(weak, strong, False) == "refuse"


def test_resolve_claim_refuses_a_near_tie():
    """Within the margin the two claims are indistinguishable — a human picks.
    Never silently overwrite."""
    m = _load()
    a = m.claim_strength(_ev("waveform", similarity=0.950))
    b = m.claim_strength(_ev("waveform", similarity=0.955))
    assert m.resolve_claim(b, a, False) == "refuse"
    assert m.resolve_claim(a, b, False) == "refuse"


def test_resolve_claim_a_duration_only_claim_never_evicts_an_incumbent():
    """A mere length match is what caused the bad link. It may take a FREE job;
    it may never take one another video already holds."""
    m = _load()
    dur_only = m.claim_strength(_ev("duration", dur_delta=0.001))
    assert m.resolve_claim(dur_only, 0.0, False) == "refuse"
    assert m.resolve_claim(dur_only, dur_only / 2.0, False) == "refuse"


def test_resolve_claim_refuses_when_the_challenger_has_no_evidence():
    m = _load()
    assert m.resolve_claim(0.0, 0.0, False) == "refuse"


def test_resolve_claim_margin_is_a_real_gap_not_noise():
    m = _load()
    assert 0.0 < m.CLAIM_MARGIN < 1.0


# ---- THE PRODUCTION FAILURE, reproduced -------------------------------------

def test_regression_the_weak_claimant_does_not_steal_the_proven_job():
    """2026-07 production, job c9f0e6c9:

      reel A — 0.005s from J's export length AND 0.947 on the waveform.
      reel B — 0.632s away, waveform inconclusive.

    Both passed the per-reel gate, so B (transcribed first) took the job A
    proves it owns. A gets J; B must be REFUSED — even though B's duration alone
    still passes the old gate.
    """
    m = _load()
    export_dur = 17.269
    j_fp = _fp(4)                     # J's exported mp4
    a_fp = _fp(4, phase=0.4)          # reel A: a re-encode of that same render
    b_fp = _fp(11)                    # reel B: a different performance entirely
    pool = [_cand("J", export_dur, j_fp)]

    ev_a = m.evidence_pick(17.274, a_fp, pool)          # delta 0.005
    ev_b = m.evidence_pick(16.637, b_fp, pool)          # delta 0.632

    assert ev_a["job_id"] == "J"
    assert "waveform" in (ev_a["source"] or "")          # the performance matched
    assert ev_a["similarity"] >= m.WAVE_MIN_SIM
    assert round(ev_a["dur_delta"], 3) == 0.005

    # B still passes the OLD per-reel gate on length alone — that is the defect.
    assert ev_b["job_id"] == "J"
    assert ev_b["source"] == "duration"
    assert round(ev_b["dur_delta"], 3) == 0.632

    sa = m.claim_strength(ev_a)
    sb = m.claim_strength(ev_b)
    assert sa > sb

    # Not a repost: two different reels, and their durations are 0.6s apart.
    repost = m.is_same_video(a_fp, b_fp, 17.274, 16.637)
    assert repost is False

    # B arrives while A holds J -> REFUSED, left for a manual pick.
    assert m.resolve_claim(sb, sa, repost) == "refuse"
    # A arrives while B holds J -> A takes it back.
    assert m.resolve_claim(sa, sb, repost) == "steal"


def test_regression_a_repost_still_links_both_reels():
    """Operator-confirmed: one export, posted twice. Two reels, identical
    duration to the bit, 0.975 waveform to the same job. Refusing the second
    would be wrong — both link, and the incumbent is left alone."""
    m = _load()
    export_dur = 46.02000045776367
    j_fp = _fp(6)
    a_fp = _fp(6, phase=0.2)          # reel 1: a re-encode of the export
    b_fp = _fp(6, phase=0.25)         # reel 2: the SAME file, posted again
    pool = [_cand("J", export_dur, j_fp)]

    ev_a = m.evidence_pick(export_dur, a_fp, pool)
    ev_b = m.evidence_pick(export_dur, b_fp, pool)
    assert ev_a["job_id"] == "J" and ev_b["job_id"] == "J"

    repost = m.is_same_video(a_fp, b_fp, export_dur, export_dur)
    assert repost is True
    assert m.resolve_claim(m.claim_strength(ev_b), m.claim_strength(ev_a), repost) == "link"
