"""v857 — ONE JOB, ONE VIDEO (except a repost).

The pure ranking (claim_strength / is_same_video / resolve_claim) is tested in
test_instagram_match.py. This file covers the DB glue that USES it:

  Layer 1: enforce_exclusivity / find_job_incumbent / release_incumbent against
           a fake session — link, steal, refuse, repost, cross-surface.
  Layer 2: source-grep that all three watchers actually route through the gate,
           and that the pool that hid already-linked jobs is gone. (py_compile
           does not catch a watcher that quietly stopped calling the gate.)
"""
import math
import os
import importlib.util

_HERE = os.path.dirname(os.path.abspath(__file__))
_CODE = os.path.dirname(_HERE)
_LT = os.path.join(_CODE, "local_transcribe.py")
_IT = os.path.join(_CODE, "instagram_transcribe.py")
_DT = os.path.join(_CODE, "drive_transcribe.py")
_MAIN = os.path.join(_CODE, "main.py")
_INDEX = os.path.join(_CODE, "static", "index.html")

import audio_fingerprint as _afp
import instagram_match as _im


def _load_lt():
    spec = importlib.util.spec_from_file_location("lt_v857_test", _LT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _fp(seed, n=120, phase=0.0):
    """A deterministic, distinctive envelope -> a real base64 fingerprint."""
    env = [
        abs(math.sin((i + phase) * (0.07 + 0.013 * seed))
            + 0.35 * math.cos((i + phase) * 0.31 * (seed + 1)))
        for i in range(n)
    ]
    norm = math.sqrt(sum(x * x for x in env)) or 1.0
    return _afp.encode_fingerprint([x / norm for x in env])


# ---- fakes ------------------------------------------------------------------

class _Job:
    def __init__(self, jid="J", dur=17.269, fp=None, instagram_video_id=None,
                 published_via="ig_match"):
        self.id = jid
        self.export_duration_s = dur
        self.export_audio_fp = fp
        self.instagram_video_id = instagram_video_id
        self.instagram_url = "https://instagram.com/reel/x" if instagram_video_id else None
        self.lifecycle_stage = "published"
        self.published_via = published_via
        self.published_at = "some-time"
        self.status = "completed"
        self.has_export = True
        self.total_clips = 3
        self.failed_clips = 0
        self.skipped_clips = 0


class _Vid:
    """Stands in for InstagramVideo / LocalVideo / DriveVideo."""
    def __init__(self, vid, dur=None, fp=None, matched_job_id=None, **kw):
        self.id = vid
        self.duration_s = dur
        self.audio_fp = fp
        self.matched_job_id = matched_job_id
        self.matched_at = "some-time" if matched_job_id else None
        self.match_score = 0.5 if matched_job_id else None
        # v857.1 — a linked video got there SOMEHOW. Default 'evidence' (the
        # unattended matcher), which is the only provenance the gate may evict;
        # the manual/filename/legacy cases pass their own.
        self.match_source = kw.get(
            "match_source", "evidence" if matched_job_id else None)
        self.shortcode = kw.get("shortcode", f"SC{vid}")
        self.file_name = kw.get("file_name", f"file{vid}.mp4")
        self.file_hash = kw.get("file_hash", f"{vid:08d}deadbeef")
        self.name = kw.get("name", f"drive{vid}.mp4")


class _Q:
    def __init__(self, rows):
        self._rows = list(rows)

    def filter(self, *a, **k):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)

    def count(self):
        return len(self._rows)


class _DB:
    """Dispatches query(Model) by class name. The helper's filters are real
    SQLAlchemy expressions; the fake ignores them and hands back the canned rows
    for that table, which is exactly the granularity these branches turn on."""
    def __init__(self, instagram=(), local=(), drive=(), jobs=(), clips=()):
        self.rows = {
            "InstagramVideo": list(instagram), "LocalVideo": list(local),
            "DriveVideo": list(drive), "Job": list(jobs), "Clip": list(clips),
        }
        self.committed = 0
        self.rolled_back = 0
        # The session's uncommitted state, as a real one would hold it: rollback
        # puts every row back the way it was found.
        self._snapshot = {
            id(row): dict(row.__dict__)
            for rows in self.rows.values() for row in rows
        }

    def query(self, *cols):
        name = getattr(cols[0], "__name__", None) or type(cols[0]).__name__
        return _Q(self.rows.get(name, []))

    def commit(self):
        self.committed += 1
        self._snapshot = {
            id(row): dict(row.__dict__)
            for rows in self.rows.values() for row in rows
        }

    def rollback(self):
        self.rolled_back += 1
        for rows in self.rows.values():
            for row in rows:
                snap = self._snapshot.get(id(row))
                if snap is not None:
                    row.__dict__.clear()
                    row.__dict__.update(snap)


def _ev(source=None, similarity=None, dur_delta=None):
    return {"job_id": "J" if source else None, "source": source,
            "similarity": similarity, "dur_delta": dur_delta, "conflict": False}


# ---- Layer 1: the gate ------------------------------------------------------

def test_a_free_job_is_linked():
    lt = _load_lt()
    job = _Job()
    me = _Vid(1, 17.274, _fp(4))
    db = _DB(jobs=[job])
    assert lt.enforce_exclusivity(db, job, me, "instagram", _ev("waveform", 0.95, 0.005)) == "link"


def test_the_weak_claimant_is_refused_and_the_incumbent_is_untouched():
    """THE PRODUCTION FAILURE. Reel A holds job J on a 0.947 waveform + a 0.005s
    length. Reel B arrives with 0.632s of length and nothing else — the old gate
    let it through, and it took the job."""
    lt = _load_lt()
    j_fp = _fp(4)
    job = _Job(dur=17.269, fp=j_fp, instagram_video_id=1)
    reel_a = _Vid(1, 17.274, _fp(4, phase=0.4), matched_job_id="J", shortcode="REEL_A")
    reel_b = _Vid(2, 16.637, _fp(11), shortcode="REEL_B")
    db = _DB(instagram=[reel_a], jobs=[job])

    verdict = lt.enforce_exclusivity(db, job, reel_b, "instagram", _ev("duration", None, 0.632))
    assert verdict == "refuse"
    # The incumbent keeps everything.
    assert reel_a.matched_job_id == "J"
    assert job.instagram_video_id == 1
    assert job.lifecycle_stage == "published"


def test_the_proven_claimant_takes_the_job_back_from_the_weak_holder():
    """The same two reels, the other transcription order: B got there first, so A
    — which PROVES it owns the job — must be able to take it."""
    lt = _load_lt()
    j_fp = _fp(4)
    job = _Job(dur=17.269, fp=j_fp, instagram_video_id=2)
    reel_b = _Vid(2, 16.637, _fp(11), matched_job_id="J", shortcode="REEL_B")
    reel_a = _Vid(1, 17.274, _fp(4, phase=0.4), shortcode="REEL_A")
    db = _DB(instagram=[reel_b], jobs=[job])

    verdict = lt.enforce_exclusivity(
        db, job, reel_a, "instagram", _ev("waveform+duration", 0.947, 0.005))
    assert verdict == "steal"
    # The incumbent is UNLINKED and the publish it caused is REVERTED — not left
    # parked in `published` with a dead link.
    assert reel_b.matched_job_id is None
    assert reel_b.matched_at is None
    assert job.instagram_video_id is None
    assert job.instagram_url is None
    assert job.lifecycle_stage == "awaiting_finishing"
    assert job.published_via is None
    assert job.published_at is None


def test_a_repost_links_both_and_leaves_the_incumbent_alone():
    """Operator-confirmed: one export, posted twice. Both reels legitimately
    claim the one job — refusing the second would be wrong."""
    lt = _load_lt()
    dur = 46.02000045776367
    job = _Job(dur=dur, fp=_fp(6), instagram_video_id=1)
    reel_1 = _Vid(1, dur, _fp(6, phase=0.2), matched_job_id="J", shortcode="POST_1")
    reel_2 = _Vid(2, dur, _fp(6, phase=0.25), shortcode="POST_2")
    db = _DB(instagram=[reel_1], jobs=[job])

    verdict = lt.enforce_exclusivity(
        db, job, reel_2, "instagram", _ev("waveform+duration", 0.975, 0.0))
    assert verdict == "link"
    assert reel_1.matched_job_id == "J"          # left alone
    assert job.instagram_video_id == 1


def test_a_waveform_proven_reel_still_backfills_onto_a_locally_published_job():
    """The export file and the reel posted from it are the SAME render on two
    surfaces — the IG matcher is DESIGNED to back-fill instagram_url onto a job
    the local/drive watcher already published. A waveform-proven cross-surface
    claim is not a rival, and must not be refused."""
    lt = _load_lt()
    job = _Job(dur=17.269, fp=_fp(4), published_via="local_watch")
    local_file = _Vid(7, 17.269, _fp(4), matched_job_id="J", file_name="final_export.mp4")
    reel = _Vid(1, 17.274, _fp(4, phase=0.4), shortcode="REEL_A")
    db = _DB(local=[local_file], jobs=[job])

    verdict = lt.enforce_exclusivity(
        db, job, reel, "instagram", _ev("waveform+duration", 0.947, 0.005))
    assert verdict == "link"
    assert local_file.matched_job_id == "J"      # the file keeps its link
    assert job.published_via == "local_watch"    # and its publish


def test_a_duration_only_reel_does_not_backfill_onto_another_surfaces_job():
    """Cross-surface is only waved through on WAVEFORM evidence. A bare length
    match is exactly the weak claim that produced the bad link."""
    lt = _load_lt()
    job = _Job(dur=17.269, fp=_fp(4), published_via="local_watch")
    local_file = _Vid(7, 17.269, _fp(4), matched_job_id="J")
    reel = _Vid(1, 16.637, _fp(11), shortcode="REEL_B")
    db = _DB(local=[local_file], jobs=[job])

    assert lt.enforce_exclusivity(
        db, job, reel, "instagram", _ev("duration", None, 0.632)) == "refuse"
    assert local_file.matched_job_id == "J"


def test_a_rival_of_our_own_kind_outranks_a_cross_surface_holder():
    """Two holders: a local file (same render, fine) and a rival reel (not). The
    rival is the conflict — it must be the one we contest."""
    lt = _load_lt()
    job = _Job(dur=17.269, fp=_fp(4), instagram_video_id=2)
    rival = _Vid(2, 16.637, _fp(11), matched_job_id="J", shortcode="REEL_B")
    local_file = _Vid(7, 17.269, _fp(4), matched_job_id="J")
    db = _DB(instagram=[rival], local=[local_file], jobs=[job])

    kind, inc = lt.find_job_incumbent(db, "J", "instagram", 1)
    assert (kind, inc.shortcode) == ("instagram", "REEL_B")


def test_a_half_written_link_is_still_an_incumbent():
    """Job.instagram_video_id and InstagramVideo.matched_job_id are two halves of
    one link. When only the job's half landed, the job is NOT free."""
    lt = _load_lt()
    job = _Job(instagram_video_id=2)
    orphan = _Vid(2, 16.637, _fp(11), shortcode="REEL_B")   # matched_job_id NOT set
    db = _DB(instagram=[orphan], jobs=[job])

    kind, inc = lt.find_job_incumbent(db, "J", "instagram", 1)
    assert (kind, inc.shortcode) == ("instagram", "REEL_B")


def test_the_incumbents_claim_is_recomputed_against_this_one_job():
    lt = _load_lt()
    j_fp = _fp(4)
    job = _Job(dur=17.269, fp=j_fp)
    proven = _Vid(1, 17.274, _fp(4, phase=0.4))
    weak = _Vid(2, 16.637, _fp(11))
    db = _DB(jobs=[job])
    assert lt.incumbent_claim_strength(db, job, proven) > \
           lt.incumbent_claim_strength(db, job, weak) > 0.0


def test_an_incumbent_with_no_media_evidence_is_never_evicted_by_a_length_match():
    """A filename-stamped link (v856) is a CERTAINTY with no fingerprint behind
    it — strength 0.0. A duration-only challenger must not be able to knock it
    out just because 0.33 > 0.0."""
    lt = _load_lt()
    job = _Job(dur=17.269, fp=_fp(4), instagram_video_id=None)
    stamped = _Vid(7, None, None, matched_job_id="J")   # no fp, no duration
    reel = _Vid(1, 17.269, None, shortcode="REEL_B")
    db = _DB(local=[stamped], jobs=[job])

    assert lt.enforce_exclusivity(
        db, job, reel, "instagram", _ev("duration", None, 0.0)) == "refuse"
    assert stamped.matched_job_id == "J"


def test_the_gate_refuses_rather_than_raising():
    """A broken gate must not link. Refusing costs one manual click; linking
    through an exception is how a wrong link gets written."""
    lt = _load_lt()

    class _Boom:
        def query(self, *a):
            raise RuntimeError("db exploded")

    assert lt.enforce_exclusivity(_Boom(), _Job(), _Vid(1), "instagram", _ev("waveform", 0.99)) \
        == "refuse"


def test_release_reverts_only_a_publish_that_was_ours():
    """A job published by DRIVE watch keeps its published state when an IG link
    is removed — unlinking a reel does not unpublish a job somebody else
    published. Mirrors main.py unmatch_video."""
    lt = _load_lt()
    job = _Job(instagram_video_id=1, published_via="drive_watch")
    reel = _Vid(1, matched_job_id="J")
    db = _DB(jobs=[job])
    reverted = lt.release_incumbent(db, job, "instagram", reel)
    assert reverted is None
    assert reel.matched_job_id is None            # the link goes
    assert job.instagram_video_id is None         # and its back-reference
    assert job.lifecycle_stage == "published"     # the publish stays
    assert job.published_via == "drive_watch"


def test_release_treats_a_null_published_via_as_an_ig_publish():
    """Links made before provenance was recorded have published_via NULL, and
    drive/local always stamp their own token — so NULL means the IG match did it."""
    lt = _load_lt()
    job = _Job(instagram_video_id=1, published_via=None)
    reel = _Vid(1, matched_job_id="J")
    db = _DB(jobs=[job])
    assert lt.release_incumbent(db, job, "instagram", reel) == "awaiting_finishing"
    assert job.published_via is None and job.published_at is None


# ---- v857.1: a human's link is not a claim ---------------------------------

def test_a_manual_link_is_never_evicted_by_the_unattended_matcher():
    """THE REPAIR-DESTROYER. The operator fixed a wrong link by hand. That link
    carries no media evidence, so it scores ~0 — and a near-duplicate build's
    reel with a 0.99 waveform would clear the gate against the same job and TAKE
    it. The displaced reel is never re-matched (transcribe_one is idempotent on
    'done'), so the repair would be destroyed, not moved. Refuse outright: a
    human's decision is not a claim to be outranked."""
    lt = _load_lt()
    job = _Job(dur=17.269, fp=_fp(4), instagram_video_id=1)
    manual = _Vid(1, None, None, matched_job_id="J", shortcode="REEL_A",
                  match_source="manual")           # hand-linked: no fp, no duration
    challenger = _Vid(2, 17.271, _fp(4, phase=0.4), shortcode="REEL_B")
    db = _DB(instagram=[manual], jobs=[job])

    verdict = lt.enforce_exclusivity(
        db, job, challenger, "instagram", _ev("waveform+duration", 0.99, 0.002))
    assert verdict == "refuse"
    assert manual.matched_job_id == "J"            # untouched
    assert manual.match_source == "manual"
    assert job.instagram_video_id == 1
    assert job.lifecycle_stage == "published"


def test_a_filename_stamped_link_is_never_evicted():
    """v856: the platform minted that name and stamped the job id into it. A
    lookup is not a claim, and no waveform can improve on an id we wrote — so a
    rival file, however strong, is refused rather than allowed to overturn it."""
    lt = _load_lt()
    job = _Job(dur=17.269, fp=_fp(4))
    stamped = _Vid(7, None, None, matched_job_id="J", match_source="filename",
                   file_name="final_export_c9f0e6c9_20260714.mp4")
    rival = _Vid(8, 17.271, _fp(4, phase=0.4), file_name="rival.mp4")
    db = _DB(local=[stamped], jobs=[job])

    assert lt.enforce_exclusivity(
        db, job, rival, "local", _ev("waveform", 0.99)) == "refuse"
    assert stamped.matched_job_id == "J"
    assert stamped.match_source == "filename"


def test_a_legacy_link_with_no_provenance_is_treated_as_manual():
    """A link written before match_source existed reads NULL. We cannot PROVE it
    was machine-made, and destroying a possibly hand-made link is far worse than
    declining a steal — so NULL is not stealable."""
    lt = _load_lt()
    job = _Job(dur=17.269, fp=_fp(4), instagram_video_id=2)
    legacy = _Vid(2, 16.637, _fp(11), matched_job_id="J", shortcode="REEL_B",
                  match_source=None)               # legacy row
    proven = _Vid(1, 17.274, _fp(4, phase=0.4), shortcode="REEL_A")
    db = _DB(instagram=[legacy], jobs=[job])

    assert lt.enforce_exclusivity(
        db, job, proven, "instagram", _ev("waveform+duration", 0.947, 0.005)) == "refuse"
    assert legacy.matched_job_id == "J"
    assert job.instagram_video_id == 2


def test_a_failed_release_rolls_back_and_the_incumbent_keeps_its_link():
    """release_incumbent clears the link FIRST, then re-derives the stage. When
    that raises, refusing without a rollback leaves the link DESTROYED and never
    replaced — and the sweep's next db.commit() (same request session) flushes
    the hole to disk."""
    lt = _load_lt()
    import lifecycle

    def _boom(*a, **k):
        raise RuntimeError("stage derivation exploded")

    original = lifecycle.derive_effective_stage
    lifecycle.derive_effective_stage = _boom
    try:
        job = _Job(dur=17.269, fp=_fp(4), instagram_video_id=2)
        weak = _Vid(2, 16.637, _fp(11), matched_job_id="J", shortcode="REEL_B")
        proven = _Vid(1, 17.274, _fp(4, phase=0.4), shortcode="REEL_A")
        db = _DB(instagram=[weak], jobs=[job])

        verdict = lt.enforce_exclusivity(
            db, job, proven, "instagram", _ev("waveform+duration", 0.947, 0.005))
    finally:
        lifecycle.derive_effective_stage = original

    assert verdict == "refuse"
    assert db.rolled_back == 1
    # Every half-applied write is undone: the incumbent still holds the job.
    assert weak.matched_job_id == "J"
    assert weak.matched_at == "some-time"
    assert job.instagram_video_id == 2
    assert job.published_via == "ig_match"


# ---- v857.1: the manual pick releases the previous holder --------------------

def test_a_manual_pick_releases_the_reel_that_was_holding_the_job():
    """Job J is held by reel A. The operator picks reel B for J. A must LET GO —
    two reels pointing at one job is the phantom that poisons the incumbent
    lookup (there is no unique constraint to stop it)."""
    lt = _load_lt()
    job = _Job(dur=17.269, fp=_fp(4), instagram_video_id=1)
    reel_a = _Vid(1, 17.274, _fp(4, phase=0.4), matched_job_id="J", shortcode="REEL_A")
    reel_b = _Vid(2, 16.637, _fp(11), shortcode="REEL_B")       # a DIFFERENT video
    db = _DB(instagram=[reel_a], jobs=[job])

    released = lt.release_other_holders(db, job, reel_b, "instagram")
    assert released == ["REEL_A"]
    assert reel_a.matched_job_id is None
    assert reel_a.matched_at is None
    assert reel_a.match_source is None
    assert job.instagram_video_id is None     # freed for the caller to write
    assert job.published_via is None          # the publish A caused is reverted


def test_a_manual_pick_leaves_a_repost_alone():
    """One export, posted twice. Both reels legitimately hold the job — picking
    the second must not unlink the first."""
    lt = _load_lt()
    dur = 46.02000045776367
    job = _Job(dur=dur, fp=_fp(6), instagram_video_id=1)
    reel_1 = _Vid(1, dur, _fp(6, phase=0.2), matched_job_id="J", shortcode="POST_1")
    reel_2 = _Vid(2, dur, _fp(6, phase=0.25), shortcode="POST_2")
    db = _DB(instagram=[reel_1], jobs=[job])

    assert lt.release_other_holders(db, job, reel_2, "instagram") == []
    assert reel_1.matched_job_id == "J"
    assert job.instagram_video_id == 1


# ---- v857.1: publishing does not steal somebody else's provenance ------------

def test_a_local_link_alongside_does_not_clobber_an_ig_publish():
    """The export file and the reel posted from it are the same render. When the
    local watcher links onto a job the IG matcher already published, overwriting
    published_via with 'local_watch' makes main.py unmatch_video refuse to revert
    its own publish — the job is then stuck in `published` forever."""
    lt = _load_lt()
    job = _Job(instagram_video_id=1, published_via="ig_match")
    local_file = _Vid(7, 17.269, _fp(4), file_name="final_export.mp4")
    db = _DB(jobs=[job])

    lt._advance_job_to_published(local_file, job, 0.95, db, match_source="evidence")
    assert job.published_via == "ig_match"        # left alone
    assert job.lifecycle_stage == "published"
    assert local_file.matched_job_id == "J"
    assert local_file.match_source == "evidence"


def test_the_local_watcher_still_claims_a_job_it_publishes_itself():
    lt = _load_lt()
    job = _Job(published_via=None)
    job.lifecycle_stage = "awaiting_finishing"
    local_file = _Vid(7, 17.269, _fp(4))
    db = _DB(jobs=[job])

    lt._advance_job_to_published(local_file, job, 0.95, db, match_source="filename")
    assert job.published_via == "local_watch"
    assert job.lifecycle_stage == "published"
    assert local_file.match_source == "filename"


# ---- Layer 2: every watcher routes through the gate -------------------------

def test_all_three_watchers_call_the_gate():
    for path in (_IT, _LT, _DT):
        src = open(path, encoding="utf-8").read()
        assert "enforce_exclusivity(" in src, f"{os.path.basename(path)} links without the gate"
        assert 'enforce_exclusivity(db, job, video, ' in src


def test_the_gate_has_exactly_one_definition():
    """Three copies of 'is somebody already holding this job' drift, and then a
    watcher starts overwriting links."""
    defs = sum(
        open(p, encoding="utf-8").read().count("def enforce_exclusivity(")
        for p in (_IT, _LT, _DT, _MAIN)
    )
    assert defs == 1


def test_the_ig_pool_no_longer_hides_an_already_linked_job():
    """The `instagram_video_id IS NULL` filter looked like an exclusivity rule
    and was not one: it made the job invisible to every reel after the first, so
    whoever was transcribed FIRST kept it — right or wrong — and the reel that
    could prove ownership never even saw it."""
    src = open(_IT, encoding="utf-8").read()
    assert "Job.instagram_video_id.is_(None)" not in src.split("def _maybe_auto_match")[1]


def test_the_suggestions_pool_no_longer_hides_an_already_linked_job():
    src = open(_MAIN, encoding="utf-8").read()
    suggest = src.split("def suggest_matches")[1].split("@app.post")[0]
    assert "Job.instagram_video_id.is_(None)" not in suggest
    assert "already_linked_to" in suggest
    assert "is_same_video(" in suggest   # a repost is not flagged


def test_every_link_site_records_how_the_link_was_made():
    """A link with NO provenance reads as 'manual' and can never be moved by the
    matcher. So every site that writes matched_job_id must stamp match_source —
    an auto-link that forgets to would freeze the job for good."""
    it_src = open(_IT, encoding="utf-8").read()
    dt_src = open(_DT, encoding="utf-8").read()
    lt_src = open(_LT, encoding="utf-8").read()
    main_src = open(_MAIN, encoding="utf-8").read()

    assert 'match_source = "evidence"' in it_src              # IG auto-match
    assert 'match_source = "evidence"' in dt_src              # drive auto-match
    assert 'match_source = "filename"' in dt_src              # drive v856 stamp
    assert 'match_source="evidence"' in lt_src                # local auto-match
    assert 'match_source="filename"' in lt_src                # local v856 stamp
    assert main_src.count('v.match_source = "manual"') == 2   # match_video + relink


def test_the_manual_pick_releases_the_old_holder_before_it_writes():
    """Order is load-bearing: releasing AFTER the write would clear the link we
    just made (release_incumbent nulls job.instagram_video_id when it still
    points at the row it is releasing)."""
    src = open(_MAIN, encoding="utf-8").read()
    body = src.split("async def match_video(")[1].split("async def unmatch_video(")[0]
    assert "release_other_holders(" in body
    assert body.index("release_other_holders(") < body.index("v.matched_job_id = job.id")


def test_releasing_a_link_has_exactly_one_definition():
    """Two copies of 'unlink the holder and undo its publish' drift, and the day
    they drift one path starts leaving a job parked in `published` forever."""
    defs = sum(
        open(p, encoding="utf-8").read().count("def release_incumbent(")
        for p in (_IT, _LT, _DT, _MAIN)
    )
    assert defs == 1


def test_the_gate_rolls_back_a_half_applied_release():
    """The except -> refuse path must not leave release_incumbent's writes behind:
    the sweep shares the request session, so the next commit would flush them."""
    src = open(_LT, encoding="utf-8").read()
    gate = src.split("def enforce_exclusivity(")[1]
    assert "db.rollback()" in gate.split("reason=gate-error")[0]


def test_the_watchers_do_not_claim_a_publish_that_is_not_theirs():
    """published_via names WHO published the job. A cross-surface link-alongside
    must not overwrite it, or unmatch_video can no longer revert its own publish
    and the job is stuck in `published`."""
    for path in (_LT, _DT):
        src = open(path, encoding="utf-8").read()
        for token in ('job.published_via = "local_watch"',
                      'job.published_via = "drive_watch"',
                      'stamped.published_via = "drive_watch"'):
            if token not in src:
                continue
            # Every write of the token is guarded by "is anyone else holding it?"
            for before in src.split(token)[:-1]:
                assert "not job.published_via" in before[-200:] or \
                       "not stamped.published_via" in before[-200:], \
                    f"{os.path.basename(path)} claims the publish unconditionally"


def test_the_popover_warns_on_a_job_another_reel_already_holds():
    src = open(_INDEX, encoding="utf-8").read()
    assert "already_linked_to" in src
    assert "already linked to" in src
    assert ".ig-suggestion-row.taken" in src
