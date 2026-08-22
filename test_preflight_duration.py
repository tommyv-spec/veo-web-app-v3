"""Tests for the pre-flight duration check.

All pure: two numbers and a comparison. No network, no audio, no models — so
the rule that decides whether a clip is set up to fail is provable in
milliseconds.

Run: python -m pytest code/test_preflight_duration.py -q
"""
import preflight_duration as p


# A 17-word / 76-char line. The v861/v884 table puts it in the 8s bucket.
LONG_LINE = "this batch sells out fast, so follow me first or it will not let me send it."
# A 10-word / 59-char line -> 4s bucket.
SHORT_LINE = "cold opens them. that is blood moving, three seconds later."


def clip(line=LONG_LINE, stored=4, status="pending", **kw):
    c = {"id": 1, "clip_index": 0, "dialogue_text": line,
         "veo_render_duration_s": stored, "status": status}
    c.update(kw)
    return c


# --- resolving what the clip will actually render at -----------------------

def test_the_clips_own_column_wins_when_it_is_set():
    assert p.effective_duration(clip(stored=6), job_duration=8) == 6


def test_a_null_column_means_the_job_level_duration_not_unknown():
    # v861 stores NULL deliberately when adaptive_duration is off, and both
    # render paths already read NULL as "use the job duration".
    assert p.effective_duration(clip(stored=None), job_duration=8) == 8


def test_null_on_both_is_genuinely_unknown():
    assert p.effective_duration(clip(stored=None), job_duration=None) is None


# --- the verdict -----------------------------------------------------------

def test_a_window_shorter_than_the_line_needs_is_under():
    r = p.check_clip(clip(line=LONG_LINE, stored=4))
    assert r["verdict"] == "under"
    assert r["needs"] == 8
    assert r["will_render_at"] == 4
    assert r["shortfall_s"] == 4


def test_a_matching_window_is_ok():
    r = p.check_clip(clip(line=LONG_LINE, stored=8))
    assert r["verdict"] == "ok" and r["shortfall_s"] == 0


def test_a_longer_window_is_over_not_a_defect():
    r = p.check_clip(clip(line=SHORT_LINE, stored=8))
    assert r["verdict"] == "over"
    assert r["needs"] == 4


def test_a_clip_with_no_line_is_unknown_not_under():
    assert p.check_clip(clip(line="", stored=4))["verdict"] == "unknown"


def test_a_clip_falling_back_to_a_too_short_job_duration_is_under():
    # The defect can live on the JOB, not the clip. Reporting the clip as
    # unknown here would hide a whole job rendering every long line at 4s.
    r = p.check_clip(clip(line=LONG_LINE, stored=None), job_duration=4)
    assert r["verdict"] == "under"
    assert r["source"] == "job"


def test_the_result_says_which_number_it_used():
    assert p.check_clip(clip(stored=6))["source"] == "clip"
    assert p.check_clip(clip(stored=None), job_duration=6)["source"] == "job"


# --- what may be fixed -----------------------------------------------------

def test_an_under_bound_clip_that_has_not_rendered_is_fixable():
    c = clip(status="pending")
    assert p.is_fixable(c, p.check_clip(c)) is True


def test_an_under_bound_clip_that_already_rendered_is_not_fixable():
    # Widening it changes nothing until something re-renders it. That is
    # clip_qc.py's job, not pre-flight's.
    c = clip(status="completed")
    assert p.is_fixable(c, p.check_clip(c)) is False


def test_a_generating_clip_is_not_touched():
    c = clip(status="generating")
    assert p.is_fixable(c, p.check_clip(c)) is False


def test_an_over_bound_clip_is_never_fixed():
    # Not a defect. Changing it would alter pacing that is fine, to save
    # money nobody asked to save.
    c = clip(line=SHORT_LINE, stored=8, status="pending")
    assert p.is_fixable(c, p.check_clip(c)) is False


def test_an_ok_clip_is_never_fixed():
    c = clip(line=LONG_LINE, stored=8, status="pending")
    assert p.is_fixable(c, p.check_clip(c)) is False


# --- the summary -----------------------------------------------------------

def _row(line, stored, status):
    c = clip(line=line, stored=stored, status=status)
    chk = p.check_clip(c)
    return {"job_id": "j", "clip_id": 1, "clip_index": 0, "status": status,
            "line": line, "check": chk, "fixable": p.is_fixable(c, chk)}


def test_the_summary_separates_preventable_from_too_late():
    rows = [
        _row(LONG_LINE, 4, "pending"),     # under, still fixable
        _row(LONG_LINE, 4, "completed"),   # under, too late
        _row(LONG_LINE, 8, "pending"),     # ok
        _row(SHORT_LINE, 8, "pending"),    # over, 4s wasted
    ]
    s = p.summarise(rows)
    assert s["clips"] == 4
    assert s["under"] == 2
    assert s["fixable_now"] == 1
    assert s["under_already_rendered"] == 1
    assert s["ok"] == 1
    assert s["over"] == 1
    assert s["wasted_seconds"] == 4
