"""Tests for clip_qc's pure half — normalisation, metrics, the verdict rule,
ranking and the backtest arithmetic.

Everything here runs without torch, ffmpeg or a network, which is the point of
keeping the scoring pure: the rule that decides whether a take gets discarded
is the part that must be provable, and it is provable in milliseconds.

Run: python -m pytest code/test_clip_qc.py -q
"""
import clip_qc as q


# --- helpers ---------------------------------------------------------------

def word(text, start, end, conf):
    return {"text": text, "start": start, "end": end, "confidence": conf}


def spoken_line(words, conf=0.9, per_word=0.30, t0=0.2):
    """A clean alignment: every word confident and a normal length."""
    out, t = [], t0
    for w in words:
        out.append(word(w, round(t, 3), round(t + per_word, 3), conf))
        t += per_word
    return out


def crushed(words, at_end=0):
    """A clean alignment whose last `at_end` words were never actually said:
    near-zero score, squeezed into a single frame. This is exactly the shape a
    cut-off Veo render produces."""
    rows = spoken_line(words)
    for i in range(len(rows) - at_end, len(rows)):
        rows[i] = word(rows[i]["text"], rows[i]["start"],
                       rows[i]["start"] + 0.012, 0.03)
    return rows


# --- normalisation ---------------------------------------------------------

def test_normalize_strips_punctuation_and_lowercases():
    assert q.normalize_words("His soldier — won't stand down!") == \
        ["his", "soldier", "won't", "stand", "down"]


def test_normalize_expands_numbers_both_ways():
    # The script writes it one way, Whisper the other; they must meet.
    assert q.normalize_words("my 45 year old husband") == \
        q.normalize_words("my forty-five year old husband")
    assert q.normalize_words("$40") == ["forty", "dollars"]
    assert q.normalize_words("30%") == ["thirty", "percent"]


def test_normalize_drops_digits_mms_cannot_align():
    # MMS_FA's alphabet is [a-z'] — a stray digit would raise inside the
    # tokenizer, so nothing numeric may survive normalisation.
    for token in q.normalize_words("day 1 to day 14"):
        assert token.replace("'", "").isalpha()


# --- which clips get scored ------------------------------------------------

def test_visual_pair_is_skipped_not_failed():
    # v698A: the silent half of a pair has no line to say. A checker that
    # scores it fails every pair in the corpus.
    clip = {"clip_role": "visual_pair", "status": "completed",
            "dialogue_text": "anything", "versions": [{"attempt": 1, "filename": "a.mp4"}]}
    ok, reason = q.should_score_clip(clip)
    assert not ok and reason == q.SKIP_VISUAL_PAIR


def test_clip_without_a_line_is_skipped():
    clip = {"status": "completed", "dialogue_text": "",
            "versions": [{"attempt": 1, "filename": "a.mp4"}]}
    ok, reason = q.should_score_clip(clip)
    assert not ok and reason == q.SKIP_NO_LINE


def test_unfinished_clip_is_skipped():
    clip = {"status": "generating", "dialogue_text": "hello",
            "versions": [{"attempt": 1, "filename": "a.mp4"}]}
    ok, reason = q.should_score_clip(clip)
    assert not ok and reason == q.SKIP_NOT_COMPLETED


def test_prompt_b_clip_scores_the_b_line_first():
    # v821: a B render spoke the reworded line. Putting A first would fail it.
    clip = {"dialogue_text": "line a", "dialogue_text_b": "line b",
            "rendered_prompt_variant": "B"}
    assert q.expected_lines(clip)[0] == ("B", "line b")
    assert q.expected_lines(clip)[1] == ("A", "line a")


def test_variant_files_dedupe_and_include_current():
    clip = {
        "versions": [{"attempt": 1, "filename": "old.mp4"},
                     {"attempt": 1, "filename": "one.mp4"},
                     {"attempt": 2, "filename": "two.mp4"}],
        "output_filename": "three.mp4", "generation_attempt": 3,
    }
    assert [v["filename"] for v in q.variant_files(clip)] == \
        ["one.mp4", "two.mp4", "three.mp4"]


# --- alignment metrics -----------------------------------------------------

def test_clean_line_is_fully_covered():
    m = q.score_alignment(spoken_line(["his", "soldier", "stood", "up"]), 2.0)
    assert m["coverage"] == 1.0
    assert m["tail_missing"] == 0
    assert m["missing"] == []


def test_cut_off_ending_shows_up_as_tail_missing():
    m = q.score_alignment(crushed(["his", "soldier", "stood", "back", "up"], at_end=2), 2.0)
    assert m["tail_missing"] == 2
    assert m["missing"] == ["back", "up"]
    assert m["coverage"] < 1.0


def test_a_dropped_word_mid_line_is_not_a_tail_miss():
    # The distinction the whole rule rests on: a mumble in the middle and a
    # clip cut short are different failures and must not share a verdict.
    rows = spoken_line(["his", "soldier", "stood", "back", "up"])
    rows[2] = word("stood", 0.8, 0.812, 0.05)
    m = q.score_alignment(rows, 2.0)
    assert m["tail_missing"] == 0
    assert m["missing"] == ["stood"]


def test_confident_but_crushed_word_counts_as_missing():
    # The aligner must place every target somewhere, so a word that is not in
    # the audio can still score well over the two frames it was squeezed into.
    assert not q.word_is_spoken(word("up", 1.0, 1.015, 0.95))


def test_long_but_unconfident_word_counts_as_missing():
    assert not q.word_is_spoken(word("up", 1.0, 1.4, 0.05))


def test_ends_flush_flags_a_hard_cut():
    rows = spoken_line(["one", "two"], per_word=0.5, t0=0.0)   # ends at 1.0
    assert q.score_alignment(rows, 1.05)["ends_flush"] is True
    assert q.score_alignment(rows, 3.0)["ends_flush"] is False


# --- transcript metrics ----------------------------------------------------

def test_transcript_of_the_same_line_has_no_errors():
    ref = ["his", "soldier", "stood", "back", "up"]
    m = q.score_transcript(ref, ref)
    assert m["wer"] == 0.0 and m["tail_unheard"] == 0


def test_transcript_missing_the_ending_reports_tail_unheard():
    ref = ["his", "soldier", "stood", "back", "up"]
    m = q.score_transcript(ref, ["his", "soldier", "stood"])
    assert m["tail_unheard"] == 2
    assert m["deleted"] == ["back", "up"]


def test_extra_spoken_words_are_insertions_not_errors_in_the_line():
    # v644 dialogue_pad puts real extra speech in the clip on purpose.
    ref = ["his", "soldier", "stood", "up"]
    m = q.score_transcript(ref, ref + ["and", "stayed", "there", "all", "night"])
    assert m["inserted"] == 5
    assert m["deleted"] == []
    assert m["tail_unheard"] == 0


def test_tail_present_survives_a_bad_patch_earlier_in_the_line():
    ref = ["american", "men", "over", "sixty", "are", "getting", "it", "back"]
    hyp = ["america", "man", "over", "sixty", "are", "getting", "it", "back"]
    assert q.tail_present_in_transcript(ref, hyp) is True


def test_tail_absent_when_the_ending_is_gone():
    ref = ["american", "men", "over", "sixty", "are", "getting", "it", "back"]
    assert q.tail_present_in_transcript(ref, ref[:4]) is False


def test_tail_present_forgives_a_misheard_final_word():
    ref = ["his", "soldier", "stood", "back", "up"]
    assert q.tail_present_in_transcript(ref, ref[:-1] + ["cup"]) is True


def test_tail_absent_even_when_the_words_before_it_matched():
    # The measured reason this check asks about the FINAL word instead of
    # searching for the last three as one string: "stood back up" scores 87
    # against "his soldier stood back" on partial_ratio, because the two words
    # that survived carry the match and the missing one disappears.
    ref = ["his", "soldier", "stood", "back", "up"]
    assert q.tail_present_in_transcript(ref, ref[:-1]) is False


def test_tail_present_when_the_pad_keeps_talking_past_the_ending():
    # v644: real extra speech follows the line. The ending is still in there.
    ref = ["i", "take", "saffron", "every", "morning"]
    assert q.tail_present_in_transcript(
        ref, ref + ["with", "warm", "milk", "before", "breakfast"]) is True


# --- the verdict -----------------------------------------------------------

def _judge(align_words, hyp, duration=3.0):
    ref = [w["text"] for w in align_words]
    a = q.score_alignment(align_words, duration)
    t = q.score_transcript(ref, hyp)
    return q.verdict(a, t, q.tail_present_in_transcript(ref, hyp))


def test_a_good_take_passes():
    words = ["his", "soldier", "stood", "back", "up"]
    v = _judge(spoken_line(words), words)
    assert v["verdict"] == "PASS" and v["hard"] == []


def test_a_truncated_take_fails_on_tail_truncated():
    words = ["his", "soldier", "stood", "back", "up"]
    v = _judge(crushed(words, at_end=2), words[:3])
    assert v["verdict"] == "FAIL"
    assert v["hard"] == ["tail_truncated"]


def test_silence_fails_as_no_speech():
    words = ["his", "soldier", "stood"]
    v = _judge(crushed(words, at_end=3), [])
    assert v["verdict"] == "FAIL"
    assert "no_speech" in v["hard"]


def test_alignment_alone_cannot_fail_a_take():
    # The mumbled-ending case: the aligner is unhappy, the transcript has the
    # words. One instrument's opinion never discards a take.
    words = ["his", "soldier", "stood", "back", "up"]
    v = _judge(crushed(words, at_end=2), words)
    assert v["verdict"] == "PASS"
    assert any("tail looks weak" in w for w in v["warnings"])


def test_transcript_alone_cannot_fail_a_take():
    # Whisper mishears the ending, the audio plainly contains it.
    words = ["his", "soldier", "stood", "back", "up"]
    v = _judge(spoken_line(words), ["his", "soldier", "stood", "black", "cup"])
    assert v["verdict"] == "PASS"


def test_a_completely_different_line_fails():
    words = ["american", "men", "over", "sixty", "are", "getting", "it", "back"]
    v = _judge(crushed(words, at_end=6),
               ["buy", "this", "saffron", "today", "please"])
    assert v["verdict"] == "FAIL"
    assert "line_missing" in v["hard"] or "tail_truncated" in v["hard"]


def test_losing_only_the_final_word_still_fails():
    # TAIL_FAIL_WORDS is 1. A video whose last word is gone is broken, and the
    # 60%-cut case in tools/verify_clip_qc_detector.py is exactly this shape:
    # one word missing, and a count of 2 let it through.
    words = ["his", "soldier", "stood", "back", "up"]
    v = _judge(crushed(words, at_end=1), words[:4])
    assert v["verdict"] == "FAIL"
    assert v["hard"] == ["tail_truncated"]


def test_a_misheard_final_word_is_not_a_shortfall():
    # The guard that lets the count sit at one: the ending is still findable,
    # so nothing fires even though the aligner and the edit path both grumble.
    words = ["his", "soldier", "stood", "back", "up"]
    v = _judge(crushed(words, at_end=1), words[:-1] + ["cup"])
    assert v["verdict"] == "PASS"


def test_padded_clip_with_a_complete_line_passes():
    words = ["it", "works"]
    v = _judge(spoken_line(words),
               words + ["and", "it", "keeps", "on", "working", "every", "day"])
    assert v["verdict"] == "PASS"


# --- ranking and backtest --------------------------------------------------

def test_ranking_puts_passing_takes_first():
    rows = [
        {"attempt": 1, "verdict": "FAIL", "score": 0.99},
        {"attempt": 2, "verdict": "PASS", "score": 0.50},
        {"attempt": 3, "verdict": "PASS", "score": 0.80},
    ]
    assert [r["attempt"] for r in q.rank_variants(rows)] == [3, 2, 1]


def test_agreement_stats_counts_false_rejects():
    clips = [
        # operator kept take 1, we would have thrown it away — a false reject
        {"selected_variant": 1, "results": [
            {"attempt": 1, "verdict": "FAIL", "score": 0.2, "hard": ["tail_truncated"]},
            {"attempt": 2, "verdict": "PASS", "score": 0.9, "hard": []}]},
        # operator kept take 2, so did we — agreement, no false reject
        {"selected_variant": 2, "results": [
            {"attempt": 1, "verdict": "FAIL", "score": 0.1, "hard": ["no_speech"]},
            {"attempt": 2, "verdict": "PASS", "score": 0.9, "hard": []}]},
    ]
    s = q.agreement_stats(clips)
    assert s["clips_with_a_chosen_take"] == 2
    assert s["would_have_rejected_the_chosen_take"] == 1
    assert s["false_reject_rate"] == 0.5
    assert s["false_reject_reasons"] == {"tail_truncated": 1}
    assert s["pick_agreement"] == 0.5


def test_score_variant_prefers_the_line_that_actually_matches():
    # A B render scored against both lines must be rescued by the B line
    # rather than failed on the A line it never spoke.
    said = ["his", "soldier", "stood", "back", "up"]
    other = ["american", "men", "are", "finally", "sleeping", "again"]
    evidence = {
        "attempt": 1, "filename": "x.mp4", "audio_duration": 3.0,
        "asr_words": said,
        "lines": [
            {"line_variant": "A", "line": "wrong", "ref_words": other,
             "aligned_words": crushed(other, at_end=6)},
            {"line_variant": "B", "line": "right", "ref_words": said,
             "aligned_words": spoken_line(said)},
        ],
    }
    r = q.score_variant(evidence)
    assert r["verdict"] == "PASS"
    assert r["line_variant"] == "B"


def test_sweep_returns_one_row_per_threshold_combination():
    said = ["his", "soldier", "stood", "back", "up"]
    clip = {"selected_variant": 1, "evidence": [{
        "attempt": 1, "filename": "x.mp4", "audio_duration": 3.0,
        "asr_words": said,
        "lines": [{"line_variant": "A", "line": "l", "ref_words": said,
                   "aligned_words": spoken_line(said)}]}]}
    grid = {"conf_floor": [0.3, 0.4], "coverage_floor": [0.7],
            "wer_ceiling": [0.5], "tail_fail_words": [2]}
    rows = q.sweep([clip], grid)
    assert len(rows) == 2
    assert all(r["variants_total"] == 1 for r in rows)


def test_build_file_syntax_in_dialogue_text_is_skipped():
    # Found on the first real run: clip 14522 carried a markdown bullet as its
    # spoken line, so a silent clip was reported as no_speech.
    clip = {"status": "completed", "dialogue_text": "- **clip_duration_s:** 4",
            "versions": [{"attempt": 1, "filename": "a.mp4"}]}
    ok, reason = q.should_score_clip(clip)
    assert not ok and reason == q.SKIP_NOT_SPEECH


def test_a_real_line_is_not_mistaken_for_build_syntax():
    for line in ["comment garlic and i will send american men the whole routine.",
                 "he's sixty-four. his soldier never stood down.",
                 "no pills. no prescription."]:
        assert q.looks_like_speech(line), line


def test_a_one_word_line_is_not_scoreable():
    assert not q.looks_like_speech("garlic")


def test_a_chosen_take_we_could_not_score_is_reported_not_dropped():
    # Real case, clip 14431: approval=approved, selected_variant=3, but only
    # attempts 1 and 2 exist in versions_json. The old code hit `continue` and
    # the clip disappeared from both the numerator and the denominator.
    clips = [{"selected_variant": 3, "approval_status": "approved", "results": [
        {"attempt": 1, "verdict": "PASS", "score": 0.9, "hard": []},
        {"attempt": 2, "verdict": "FAIL", "score": 0.2, "hard": ["line_missing"]}]}]
    s = q.agreement_stats(clips)
    assert s["chosen_take_not_scored"] == 1
    assert s["chosen_take_not_scored_approved"] == 1
    assert s["clips_with_a_chosen_take"] == 0
    assert s["approved_clips"] == 0
    assert s["approved_takes_flagged"] == 0


# --- the agreement ledger --------------------------------------------------

def _clip(qc=None, status="approved", selected=1, clip_id=1):
    return {"id": clip_id, "job_id": "j", "approval_status": status,
            "selected_variant": selected, "qc": qc}


def _qc(verdict="PASS", recommended=1, state="pending_review", takes=1):
    return {"version": 1, "checker": "v939", "scored_at": "2026-08-22T00:00:00+00:00",
            "verdict": verdict, "recommended_attempt": recommended,
            "operator_state_at_scoring": state, "line": "a line",
            "takes": [{"attempt": i + 1, "hard": []} for i in range(takes)]}


def test_a_clip_you_have_not_reviewed_is_not_resolved():
    # Counting an unreviewed clip as agreement would count silence as a yes.
    assert q.resolve_clip(_clip(_qc(), status="pending_review"), "now") is None


def test_a_clip_with_no_report_is_not_resolved():
    assert q.resolve_clip(_clip(None), "now") is None


def test_agreement_when_machine_and_operator_both_keep():
    row = q.resolve_clip(_clip(_qc("PASS")), "now")
    assert row["keep_agreement"] is True and row["prospective"] is True


def test_disagreement_when_machine_would_discard_what_you_kept():
    row = q.resolve_clip(_clip(_qc("FAIL")), "now")
    assert row["keep_agreement"] is False
    assert row["machine_kept"] is False and row["operator_kept"] is True


def test_a_report_written_after_you_approved_is_marked_retrospective():
    # Scoring a clip the operator already blessed measures nothing about
    # whether the machine could have saved them the review.
    row = q.resolve_clip(_clip(_qc(state="approved")), "now")
    assert row["prospective"] is False


def test_pick_agreement_is_none_when_there_was_only_one_take():
    row = q.resolve_clip(_clip(_qc(takes=1)), "now")
    assert row["pick_meaningful"] is False and row["pick_agreement"] is None


def test_pick_agreement_counts_only_when_there_was_a_choice():
    row = q.resolve_clip(_clip(_qc(recommended=2, takes=2), selected=2), "now")
    assert row["pick_meaningful"] is True and row["pick_agreement"] is True


def test_scorecard_separates_prospective_from_retrospective():
    rows = [
        q.resolve_clip(_clip(_qc("PASS"), clip_id=1), "now"),
        q.resolve_clip(_clip(_qc("FAIL"), clip_id=2), "now"),
        q.resolve_clip(_clip(_qc("PASS", state="approved"), clip_id=3), "now"),
    ]
    card = q.scorecard(rows)
    assert card["prospective"]["clips"] == 2
    assert card["retrospective"]["clips"] == 1
    assert card["prospective"]["would_have_discarded_a_kept_clip"] == 1
    assert card["prospective"]["keep_agreement"] == 0.5


def test_scorecard_does_not_double_count_a_reresolved_clip():
    rows = [q.resolve_clip(_clip(_qc("PASS"), clip_id=7), "t1"),
            q.resolve_clip(_clip(_qc("PASS"), clip_id=7), "t2")]
    card = q.scorecard(rows)
    assert card["total_rows"] == 2 and card["distinct_clips"] == 1
    assert card["prospective"]["clips"] == 1


def test_ledger_round_trip_is_append_only(tmp_path):
    p = tmp_path / "ledger.jsonl"
    q.ledger_append([{"clip_id": 1, "keep_agreement": True}], p)
    q.ledger_append([{"clip_id": 2, "keep_agreement": False}], p)
    rows = q.ledger_rows(p)
    assert [r["clip_id"] for r in rows] == [1, 2]


def test_ledger_survives_a_corrupt_line(tmp_path):
    p = tmp_path / "ledger.jsonl"
    q.ledger_append([{"clip_id": 1}], p)
    with p.open("a", encoding="utf-8") as fh:
        fh.write("{not json\n")
    q.ledger_append([{"clip_id": 2}], p)
    assert [r["clip_id"] for r in q.ledger_rows(p)] == [1, 2]


def test_report_records_the_state_the_operator_was_in():
    clip = {"id": 5, "dialogue_text": "his soldier stood back up",
            "selected_variant": 1, "approval_status": "pending_review"}
    results = [{"attempt": 1, "verdict": "PASS", "score": 0.9, "hard": [],
                "warnings": [], "alignment": {"coverage": 1.0, "tail_missing": 0,
                                              "missing": []},
                "transcript": {"wer": 0.0}, "asr_text": "his soldier stood back up"}]
    rep = q.build_report(clip, results, "2026-08-22T00:00:00+00:00")
    assert rep["version"] == 1
    assert rep["operator_state_at_scoring"] == "pending_review"
    assert rep["recommended_attempt"] == 1
    assert rep["verdict"] == "PASS"


def test_report_recommends_the_passing_take_over_a_higher_scoring_failure():
    clip = {"id": 6, "dialogue_text": "a line", "selected_variant": 1,
            "approval_status": "pending_review"}
    results = [
        {"attempt": 1, "verdict": "FAIL", "score": 0.99, "hard": ["tail_truncated"],
         "warnings": [], "alignment": {}, "transcript": {}, "asr_text": ""},
        {"attempt": 2, "verdict": "PASS", "score": 0.60, "hard": [],
         "warnings": [], "alignment": {}, "transcript": {}, "asr_text": ""},
    ]
    rep = q.build_report(clip, results, "t")
    assert rep["recommended_attempt"] == 2
    # The clip-level verdict describes the take the operator is looking at.
    assert rep["verdict"] == "FAIL"


def test_the_first_report_wins_so_a_prospective_call_cannot_be_rewritten(tmp_path, monkeypatch):
    # Rescoring a clip the operator has since approved must not quietly replace
    # a prospective call with a retrospective one.
    monkeypatch.setattr(q, "REPORT_DIR", tmp_path)
    q.save_local_report(42, {"version": 1, "operator_state_at_scoring": "pending_review"})
    q.save_local_report(42, {"version": 1, "operator_state_at_scoring": "approved"})
    assert q.load_local_report(42)["operator_state_at_scoring"] == "pending_review"


def test_a_missing_or_corrupt_local_report_reads_as_none(tmp_path, monkeypatch):
    monkeypatch.setattr(q, "REPORT_DIR", tmp_path)
    assert q.load_local_report(99) is None
    (tmp_path / "99.json").write_text("{not json", encoding="utf-8")
    assert q.load_local_report(99) is None


# --- removal safety --------------------------------------------------------

def _fail_clip(status="pending_review", hard=("tail_truncated",), clip_id=1):
    return {"id": clip_id, "job_id": "j", "clip_index": 0,
            "approval_status": status, "selected_variant": 1,
            "qc": {"version": 1, "verdict": "FAIL", "line": "a line",
                   "takes": [{"attempt": 1, "hard": list(hard), "heard": "a",
                              "coverage": 0.4, "tail_missing": 3}]}}


def test_an_approved_clip_is_never_removed():
    # The rule the whole feature rests on: a human decision is not overturned.
    rm, pr = q.discard_candidates([_fail_clip(status="approved")])
    assert rm == []
    assert len(pr) == 1 and pr[0]["status"] == "approved"


def test_a_rejected_clip_is_also_left_alone():
    rm, pr = q.discard_candidates([_fail_clip(status="rejected")])
    assert rm == [] and len(pr) == 1


def test_an_unreviewed_failing_clip_is_removable():
    rm, pr = q.discard_candidates([_fail_clip()])
    assert len(rm) == 1 and pr == []
    assert rm[0]["hard"] == ["tail_truncated"]


def test_a_passing_clip_is_never_removed():
    clip = _fail_clip()
    clip["qc"]["verdict"] = "PASS"
    assert q.discard_candidates([clip]) == ([], [])


def test_a_clip_with_no_report_is_never_removed():
    clip = _fail_clip()
    clip["qc"] = None
    assert q.discard_candidates([clip]) == ([], [])


def test_reasons_filter_narrows_what_can_be_removed():
    clip = _fail_clip(hard=("line_missing",))
    assert q.discard_candidates([clip], ["tail_truncated"]) == ([], [])
    rm, _ = q.discard_candidates([clip], ["line_missing"])
    assert len(rm) == 1


def test_removal_reads_the_take_the_operator_is_looking_at():
    # Two takes; the SELECTED one is fine, an older one failed. Nothing to do.
    clip = _fail_clip()
    clip["selected_variant"] = 2
    clip["qc"]["takes"].append({"attempt": 2, "hard": [], "heard": "full line",
                                "coverage": 1.0, "tail_missing": 0})
    assert q.discard_candidates([clip]) == ([], [])


def test_a_one_word_tail_gap_flags_but_does_not_remove():
    # Measured: a paraphrase ending on a synonym ("never hear about" ->
    # "never learn") scores tail_missing 1 and is perfectly usable.
    clip = _fail_clip()
    clip["qc"]["takes"][0]["tail_missing"] = 1
    assert q.discard_candidates([clip]) == ([], [])


def test_a_real_truncation_still_removes():
    clip = _fail_clip()
    clip["qc"]["takes"][0]["tail_missing"] = 4
    rm, _ = q.discard_candidates([clip])
    assert len(rm) == 1


def test_paraphrase_is_not_removed_by_default():
    # line_missing is out of the default set on purpose.
    clip = _fail_clip(hard=("line_missing",))
    clip["qc"]["takes"][0]["tail_missing"] = 0
    assert q.discard_candidates([clip]) == ([], [])
    rm, _ = q.discard_candidates([clip], ["no_speech", "tail_truncated", "line_missing"])
    assert len(rm) == 1


def test_no_speech_removes_regardless_of_tail_length():
    clip = _fail_clip(hard=("no_speech",))
    clip["qc"]["takes"][0]["tail_missing"] = 0
    rm, _ = q.discard_candidates([clip])
    assert len(rm) == 1


# --- the operator's ruling: a cut clip they approved is a CATCH ------------

def _approved_fail(hard, clip_id=1):
    return {"clip_id": clip_id, "approval_status": "approved",
            "selected_variant": 1, "line": "l", "asr_text": "h",
            "results": [{"attempt": 1, "verdict": "FAIL", "score": 0.2,
                         "hard": list(hard), "alignment": {}, "transcript": {}}]}


def test_a_cut_clip_you_approved_counts_as_a_catch_not_an_error():
    # Operator 2026-08-23: "if i pick something that was cut was a mistake,
    # good catch." Counting it as a false reject makes the machine look wrong
    # for being right.
    s = q.agreement_stats([_approved_fail(["tail_truncated"])])
    assert s["caught_a_cut_clip_you_approved"] == 1
    assert s["disagreed_on_a_paraphrase_you_approved"] == 0


def test_a_paraphrase_you_approved_is_an_open_question_not_a_catch():
    s = q.agreement_stats([_approved_fail(["line_missing"])])
    assert s["caught_a_cut_clip_you_approved"] == 0
    assert s["disagreed_on_a_paraphrase_you_approved"] == 1


def test_a_clip_that_is_both_counts_as_a_catch():
    # Cut is the settled reason; it wins over the unsettled one.
    s = q.agreement_stats([_approved_fail(["tail_truncated", "line_missing"])])
    assert s["caught_a_cut_clip_you_approved"] == 1
    assert s["disagreed_on_a_paraphrase_you_approved"] == 0


def test_an_approved_cut_clip_can_be_sent_back_when_asked():
    clip = _fail_clip(status="approved")
    clip["qc"]["takes"][0]["tail_missing"] = 4
    assert q.discard_candidates([clip])[0] == []          # off by default
    rm, _ = q.discard_candidates([clip], include_approved_cuts=True)
    assert len(rm) == 1 and rm[0]["overriding_approval"] is True


def test_an_approved_paraphrase_is_never_sent_back_even_with_the_flag():
    # Their taste is not a defect, whatever flags are set.
    clip = _fail_clip(status="approved", hard=("line_missing",))
    clip["qc"]["takes"][0]["tail_missing"] = 0
    rm, pr = q.discard_candidates(
        [clip], ["no_speech", "tail_truncated", "line_missing"],
        include_approved_cuts=True)
    assert rm == [] and len(pr) == 1


def test_the_report_carries_the_fields_the_diagnosis_needs():
    clip = {"id": 5, "dialogue_text": "a line here", "selected_variant": 1,
            "approval_status": "pending_review"}
    results = [{"attempt": 1, "verdict": "FAIL", "score": 0.4,
                "hard": ["tail_truncated"], "warnings": [],
                "audio_duration": 4.011,
                "alignment": {"coverage": 0.6, "tail_missing": 3,
                              "missing": ["x"], "tail_room_s": 1.103},
                "transcript": {"wer": 0.3}, "asr_text": "a line"}]
    take = q.build_report(clip, results, "t")["takes"][0]
    # Without these two, starved and abandoned are indistinguishable.
    assert take["tail_room_s"] == 1.103
    assert take["audio_duration"] == 4.011


def _take(tail_missing=3, tail_room=1.0, coverage=0.6, duration=4.0):
    return {"attempt": 1, "verdict": "FAIL", "hard": ["tail_truncated"],
            "tail_missing": tail_missing, "tail_room_s": tail_room,
            "coverage": coverage, "audio_duration": duration}


def test_a_clip_rendered_shorter_than_the_table_is_under_bucketed():
    # clip 14303: 17 words, table says 8s, it rendered at 4s.
    d = q.diagnose_cut("this batch sells out fast, so follow me first or it will not let me send it.",
                       _take(tail_missing=6, tail_room=0.48, duration=4.0))
    assert d["diagnosis"] == "under_bucketed"
    assert d["table_duration"] == 8
    assert d["rendered_duration"] == 4


def test_a_clip_that_ran_to_the_very_end_is_starved():
    # Correct bucket, but the speech is still going when the audio stops.
    d = q.diagnose_cut("i was just like you. then i found this young korean healer.",
                       _take(tail_missing=2, tail_room=0.05, duration=6.0))
    assert d["diagnosis"] == "starved"


def test_a_clip_with_unused_silence_was_abandoned():
    # clip 14274: correct 6s bucket, 2.04s of silence left, still lost 6 words.
    d = q.diagnose_cut("before you drink it, twist open one saffron capsule and stir the red threads in.",
                       _take(tail_missing=6, tail_room=2.04, duration=6.0))
    assert d["diagnosis"] == "abandoned"
    assert d["table_duration"] == d["rendered_duration"] == 6


def test_a_clip_that_is_not_cut_has_no_diagnosis():
    take = _take()
    take["hard"] = ["line_missing"]
    assert q.diagnose_cut("some line", take)["diagnosis"] is None


def test_an_old_report_without_the_timing_fields_is_unknown_not_guessed():
    # 128 reports were written before Task 1 added tail_room_s. Treating a
    # missing field as 0.0 would label every one of them "starved" and widen
    # clips that already had unused time.
    old = {"attempt": 1, "hard": ["tail_truncated"], "tail_missing": 3,
           "coverage": 0.6}          # no tail_room_s, no audio_duration
    assert q.diagnose_cut("some line here", old)["diagnosis"] == "unknown"


# --- all_takes_cut ----------------------------------------------------------

def test_all_takes_cut_is_false_when_one_take_is_clean():
    takes = [{"attempt": 1, "hard": ["tail_truncated"]},
             {"attempt": 2, "hard": []}]
    assert q.all_takes_cut(takes) is False


def test_all_takes_cut_is_true_when_every_take_is_cut():
    takes = [{"attempt": 1, "hard": ["tail_truncated"]},
             {"attempt": 2, "hard": ["tail_truncated", "line_missing"]}]
    assert q.all_takes_cut(takes) is True


def test_a_single_cut_take_counts_as_all_takes_cut():
    # 98% of clips have exactly one take; requiring two would make this
    # feature fire almost never.
    assert q.all_takes_cut([{"attempt": 1, "hard": ["tail_truncated"]}]) is True


def test_no_takes_is_not_all_cut():
    assert q.all_takes_cut([]) is False


# --- plan_duration_repair ----------------------------------------------------

def test_under_bucketed_repairs_to_the_table_duration():
    plan = q.plan_duration_repair({"diagnosis": "under_bucketed",
                                   "table_duration": 8, "rendered_duration": 4})
    assert plan["action"] == "widen_and_redo"
    assert plan["new_duration"] == 8


def test_starved_repairs_to_the_next_bucket_up():
    plan = q.plan_duration_repair({"diagnosis": "starved",
                                   "table_duration": 6, "rendered_duration": 6})
    assert plan["action"] == "widen_and_redo"
    assert plan["new_duration"] == 8


def test_abandoned_rerolls_without_touching_the_duration():
    # It already had unused time; a longer window cannot help.
    plan = q.plan_duration_repair({"diagnosis": "abandoned",
                                   "table_duration": 6, "rendered_duration": 6})
    assert plan["action"] == "redo_same_duration"
    assert plan["new_duration"] is None


def test_a_starved_clip_already_at_the_top_bucket_needs_a_shorter_line():
    # 10s is the last bucket. There is no wider window to buy.
    plan = q.plan_duration_repair({"diagnosis": "starved",
                                   "table_duration": 10, "rendered_duration": 10})
    assert plan["action"] == "shorten_the_line"
    assert plan["new_duration"] is None
    assert "no longer bucket" in plan["why"]


def test_a_clip_that_was_not_cut_needs_no_repair():
    plan = q.plan_duration_repair({"diagnosis": None})
    assert plan["action"] is None


def test_an_undiagnosable_clip_is_re_rolled_never_widened():
    # "unknown" means we could not tell starved from abandoned. Widening on a
    # guess is the one move that can waste a render AND change good pacing.
    plan = q.plan_duration_repair({"diagnosis": "unknown"})
    assert plan["action"] == "redo_same_duration"
    assert plan["new_duration"] is None


def test_all_takes_cut_at_the_top_bucket_escalates_to_shorten_the_line():
    # The operator's own trigger: every take cut, and no wider window to buy.
    plan = q.plan_duration_repair({"diagnosis": "starved", "table_duration": 10,
                                   "rendered_duration": 10}, all_cut=True)
    assert plan["action"] == "shorten_the_line"
    assert "every take" in plan["why"]
