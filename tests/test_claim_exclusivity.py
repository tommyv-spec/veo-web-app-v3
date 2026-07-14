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
        self.shortcode = kw.get("shortcode", f"SC{vid}")
        self.file_name = kw.get("file_name", f"file{vid}.mp4")
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

    def query(self, *cols):
        name = getattr(cols[0], "__name__", None) or type(cols[0]).__name__
        return _Q(self.rows.get(name, []))

    def commit(self):
        self.committed += 1


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


def test_the_popover_warns_on_a_job_another_reel_already_holds():
    src = open(_INDEX, encoding="utf-8").read()
    assert "already_linked_to" in src
    assert "already linked to" in src
    assert ".ig-suggestion-row.taken" in src
