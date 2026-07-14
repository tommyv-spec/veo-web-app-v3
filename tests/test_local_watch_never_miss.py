"""v822 — local-folder watcher never-miss hardening.

Layer 1: pure helpers in local_transcribe (importable standalone).
Layer 2: source-grep-assert endpoint + frontend markers (this codebase has
been bitten by missing-name regressions py_compile does not catch).
"""
import os
from datetime import datetime, timedelta

import importlib.util

_HERE = os.path.dirname(os.path.abspath(__file__))
_CODE = os.path.dirname(_HERE)
_LT = os.path.join(_CODE, "local_transcribe.py")
_MAIN = os.path.join(_CODE, "main.py")
_INDEX = os.path.join(_CODE, "static", "index.html")


def _load_lt():
    spec = importlib.util.spec_from_file_location("lt_v822_test", _LT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# ---- Layer 1: should_reprocess ------------------------------------------
def test_failed_always_reprocesses():
    lt = _load_lt()
    assert lt.should_reprocess("failed", datetime.utcnow()) is True


def test_done_never_reprocesses():
    lt = _load_lt()
    old = datetime.utcnow() - timedelta(hours=5)
    assert lt.should_reprocess("done", old) is False


def test_fresh_pending_not_reprocessed():
    lt = _load_lt()
    fresh = datetime.utcnow() - timedelta(seconds=30)
    assert lt.should_reprocess("pending", fresh) is False
    assert lt.should_reprocess("running", fresh) is False


def test_stuck_pending_reprocessed():
    lt = _load_lt()
    stuck = datetime.utcnow() - timedelta(minutes=11)
    assert lt.should_reprocess("pending", stuck) is True
    assert lt.should_reprocess("running", stuck) is True


def test_pending_without_created_at_reprocessed():
    lt = _load_lt()
    assert lt.should_reprocess("pending", None) is True


# ---- Layer 2: endpoint + wiring symbols -----------------------------------
def test_upload_endpoint_uses_should_reprocess():
    src = open(_MAIN, encoding="utf-8").read()
    assert "should_reprocess(" in src
    assert "/api/local-videos/rematch" in src


def test_rematch_helper_defined_and_scoped():
    src = open(_LT, encoding="utf-8").read()
    assert "def rematch_unmatched(user_id, db" in src
    assert 'transcription_status == "done"' in src
    assert "matched_job_id == None" in src


# ---- Layer 2: frontend markers --------------------------------------------
def test_frontend_stability_gate_and_cache():
    src = open(_INDEX, encoding="utf-8").read()
    assert "_localPendingStability" in src
    assert "_localStatCache" in src
    assert "_localMissingStreak" in src
    assert "_localDeleteEligible" in src


def test_frontend_timeout_and_rematch_wiring():
    src = open(_INDEX, encoding="utf-8").read()
    assert "function _fetchT(" in src
    assert '"/api/local-videos/rematch"' in src
    assert "_localRowRetryable" in src
    assert "visibilitychange" in src


def test_frontend_poll_path_noninteractive_permission():
    src = open(_INDEX, encoding="utf-8").read()
    assert "_verifyDirPermission(_localDirHandle, false)" in src
    assert "_localPermissionLost" in src


def test_frontend_dead_upload_helper_removed():
    src = open(_INDEX, encoding="utf-8").read()
    assert "_uploadLocalFile" not in src


def test_frontend_review_fixes_v822_1():
    """Post-review fixes: UTC parse (naive isoformat read as local time broke
    stuck-row retry east of UTC), retry cap, scan-generation guard."""
    src = open(_INDEX, encoding="utf-8").read()
    assert "function _localParseUtc(" in src
    assert "LOCAL_MAX_RETRY_ATTEMPTS" in src
    assert "_localRetryAttempts" in src
    assert "_localScanGen" in src
    assert "gen !== _localScanGen" in src


# ---- v822.3: sweep DoS fix (bulk dialogue + bounded sweep) ----------------
class _FakeQuery:
    def __init__(self, rows):
        self._rows = list(rows)
    def filter(self, *a, **k):
        return self
    def order_by(self, *a, **k):
        return self
    def limit(self, n):
        self._rows = self._rows[:n]
        return self
    def all(self):
        return self._rows
    def first(self):
        return self._rows[0] if self._rows else None


class _FakeDB:
    """Returns the SAME canned rows for every query() — enough for the
    grouping + cooldown/empty-pool guards, which never depend on WHICH cols.

    Exception: the v852 lineup query selects Job columns, so it gets its own
    canned rows (a clip tuple unpacked as (job_id, clip_order_json) would be
    nonsense)."""
    def __init__(self, rows, job_rows=None):
        self._rows = rows
        self._job_rows = list(job_rows or [])
    def query(self, *cols):
        owners = {
            getattr(c, "class_", None).__name__
            for c in cols if getattr(c, "class_", None) is not None
        }
        if owners == {"Job"}:
            return _FakeQuery(self._job_rows)
        return _FakeQuery(self._rows)


def _row(job_id, clip_id=1, clip_index=0, role=None, paired=None,
         dt="", dtb=None, variant="A", vo=None, approval="approved",
         status="completed"):
    """Positional row tuple matching the _bulk_dialogue_map query column order."""
    return (clip_id, job_id, clip_index, role, paired, dt, dtb, variant, vo,
            approval, status)


def test_bulk_dialogue_map_groups_and_coalesces():
    lt = _load_lt()
    rows = [
        _row("j1", clip_id=1, clip_index=0, dt="hello"),
        _row("j1", clip_id=2, clip_index=1, dt="", vo="world"),   # voiceover_line preferred
        _row("j2", clip_id=3, clip_index=0, dt="solo"),
    ]
    m = lt._bulk_dialogue_map(_FakeDB(rows), ["j1", "j2"])
    assert m["j1"] == "hello world"
    assert m["j2"] == "solo"


def test_bulk_dialogue_map_uses_prompt_b_reworded_line():
    """v852: a clip that rendered via Prompt B speaks dialogue_text_b."""
    lt = _load_lt()
    rows = [
        _row("j1", clip_id=1, clip_index=0, dt="the banned wording",
             dtb="the reworded line actually spoken", variant="B"),
    ]
    m = lt._bulk_dialogue_map(_FakeDB(rows), ["j1"])
    assert m["j1"] == "the reworded line actually spoken"


def test_bulk_dialogue_map_excludes_rejected_clips():
    """v852: only clips that made the final cut are in the exported video."""
    lt = _load_lt()
    rows = [
        _row("j1", clip_id=1, clip_index=0, dt="kept", approval="approved"),
        _row("j1", clip_id=2, clip_index=1, dt="rejected", approval="rejected"),
    ]
    m = lt._bulk_dialogue_map(_FakeDB(rows), ["j1"])
    assert m["j1"] == "kept"


def test_bulk_dialogue_map_keeps_a_pending_clip_the_export_still_contains():
    """v852 fix: a post-export redo flips an approved clip back to
    pending_review (main.py:12963) but the posted mp4 still speaks that line.
    The builder must keep it — losing a whole line hurts the cosine more than
    a slightly stale one."""
    lt = _load_lt()
    rows = [
        _row("j1", clip_id=1, clip_index=0, dt="first line", approval="approved"),
        _row("j1", clip_id=2, clip_index=1, dt="redone line", approval="pending_review"),
    ]
    m = lt._bulk_dialogue_map(_FakeDB(rows), ["j1"])
    assert m["j1"] == "first line redone line"


def test_bulk_dialogue_map_drops_clips_that_never_rendered():
    """v852 fix: failed / skipped clips are not in the export."""
    lt = _load_lt()
    rows = [
        _row("j1", clip_id=1, clip_index=0, dt="rendered", status="completed"),
        _row("j1", clip_id=2, clip_index=1, dt="never rendered", status="failed"),
    ]
    m = lt._bulk_dialogue_map(_FakeDB(rows), ["j1"])
    assert m["j1"] == "rendered"


def test_bulk_dialogue_map_honours_a_custom_lineup():
    """v852 fix: a lineup export selects clips by id with NO approval filter
    (main.py:9232-9241), so the lineup IS the final cut."""
    lt = _load_lt()
    rows = [
        _row("j1", clip_id=1, clip_index=0, dt="in the lineup", approval="pending_review"),
        _row("j1", clip_id=2, clip_index=1, dt="approved but cut", approval="approved"),
    ]
    m = lt._bulk_dialogue_map(_FakeDB(rows, job_rows=[("j1", "[1]")]), ["j1"])
    assert m["j1"] == "in the lineup"


def test_bulk_dialogue_map_malformed_lineup_does_not_raise():
    lt = _load_lt()
    rows = [_row("j1", clip_id=1, clip_index=0, dt="only line")]
    m = lt._bulk_dialogue_map(_FakeDB(rows, job_rows=[("j1", "not json{")]), ["j1"])
    assert m["j1"] == "only line"


def test_bulk_dialogue_map_broll_pair_uses_the_audio_twin():
    """v852: the audio_pair twin RENDERS the speech. When it fell back to
    Prompt B, the visual twin's voiceover_line is stale. The builder must fetch
    audio_pair rows (no SQL exclusion) so the pair can be resolved."""
    lt = _load_lt()
    rows = [
        _row("j1", clip_id=10, clip_index=0, role="visual_pair",
             dt="", vo="the stale original line"),
        _row("j1", clip_id=11, clip_index=100000, role="audio_pair", paired=10,
             dt="the stale original line",
             dtb="the reworded line actually spoken", variant="B"),
    ]
    m = lt._bulk_dialogue_map(_FakeDB(rows), ["j1"])
    assert m["j1"] == "the reworded line actually spoken"


def test_bulk_dialogue_map_empty_ids_no_query():
    lt = _load_lt()
    # empty id list short-circuits BEFORE touching db (pass a db that would
    # raise if queried).
    class _Boom:
        def query(self, *a):
            raise AssertionError("must not query on empty ids")
    assert lt._bulk_dialogue_map(_Boom(), []) == {}


def test_sweep_cooldown_blocks_second_immediate_call():
    lt = _load_lt()
    uid = "cooldown-user-xyz"
    db = _FakeDB([])  # empty candidate pool -> first call returns checked 0
    first = lt.rematch_unmatched(uid, db)
    assert first.get("checked") == 0
    second = lt.rematch_unmatched(uid, db)
    assert second.get("skipped") == "cooldown"


def test_sweep_guard_constants_present():
    lt = _load_lt()
    assert lt._SWEEP_BUDGET_S > 0
    assert lt._SWEEP_MAX_VIDEOS > 0
    assert lt._SWEEP_MAX_AGE_H > 0
    assert lt._SWEEP_COOLDOWN_S > 0


def test_no_per_job_n_plus_one_in_sweep_source():
    """The old per-job _full_dialogue N+1 (Clip query inside the candidate
    loop) must be gone; the bulk map + advance helper must exist."""
    src = open(_LT, encoding="utf-8").read()
    assert "def _bulk_dialogue_map(" in src
    assert "def _advance_job_to_published(" in src
    assert "def _full_dialogue(job):" not in src  # the N+1 shape is deleted
    assert "dialogue_map=dialogue_map" in src     # sweep shares one map


def test_local_matcher_uses_tfidf_margin_gate_v822_4():
    """local_transcribe must use rank_tfidf + auto_pick (not the char-level
    best_matches) and carry env-tunable HIGH/MARGIN thresholds."""
    src = open(_LT, encoding="utf-8").read()
    assert "rank_tfidf(" in src
    assert "auto_pick(" in src
    assert "_MATCH_HIGH" in src
    assert "_MATCH_MARGIN" in src
    assert "LOCAL_MATCH_HIGH" in src
    # the old char-level path must be gone from the local matcher
    assert "best_matches(" not in src


def test_local_matcher_uses_idf_power_v822_6():
    """v822.6: the local ranker passes the tuned idf_power that won the bake-off."""
    src = open(_LT, encoding="utf-8").read()
    assert "_MATCH_IDF_POWER" in src
    assert "LOCAL_MATCH_IDF_POWER" in src
    assert "idf_power=_MATCH_IDF_POWER" in src


def test_suggest_endpoint_uses_tfidf_not_char_v822_6():
    """v822.6: the manual-suggest endpoint uses rank_tfidf (idf_power), not
    the old char-level best_matches / per-candidate score() N+1 loop."""
    src = open(_MAIN, encoding="utf-8").read()
    assert "rank_tfidf(v.transcription" in src
    assert "_ig_match.score(_t, _dlg)" not in src
