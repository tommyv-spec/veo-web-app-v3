# -*- coding: utf-8 -*-
"""Local folder uploads → transcribe → match → advance.

Browser-based watcher (File System Access API) detects new files in a
user-picked folder + POSTs each as multipart to /api/local-videos/upload.
This module handles the server-side pipeline:
  1. Save the uploaded blob to a tempfile.
  2. ffmpeg → mono 16k WAV.
  3. faster-whisper → transcript.
  4. Score against the user's awaiting_finishing jobs.
  5. On a CONFIDENT match (TF-IDF + the _MATCH_HIGH/_MATCH_MARGIN gate),
     advance the job to published with published_via='local_watch'.

Mirrors drive_transcribe.py shape so the matching logic stays consistent.
"""
import os
import subprocess
import tempfile
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session

# Reuse instagram_transcribe's cached Whisper model — same process, single
# load. faster-whisper is heavyweight so this matters for cold-start cost.
# fingerprint_downloaded_media (v855) is shared for the same reason: one
# definition of "stamp this row with the media evidence of the file on disk".
from instagram_transcribe import _model as _whisper_model
from instagram_transcribe import fingerprint_downloaded_media as _fingerprint_media

# v822.4 matcher gate (TF-IDF cosine + margin). Env-tunable WITHOUT a deploy so
# the operator can calibrate from the `... s1=/s2=/margin=/decision=` log lines.
# HIGH = min cosine for the top candidate; MARGIN = how far the top must beat
# the runner-up to auto-publish (near-duplicate twins sit within MARGIN ->
# deferred to manual pick, never auto-matched to the wrong one).
# v853: these are now THE thresholds for every auto-publishing matcher — the
# local watcher, the drive watcher and the IG watcher all import them.
_MATCH_HIGH = float(os.environ.get("LOCAL_MATCH_HIGH", "0.50"))
_MATCH_MARGIN = float(os.environ.get("LOCAL_MATCH_MARGIN", "0.12"))
# v822.6: exponent on IDF. 2.0 (rare-term-weighted cosine) beat plain TF-IDF
# and BM25 on the operator's real data — same #1 accuracy (48/59 stored#1,
# 57/59 top-3), a wider margin (0.37 vs 0.32), and 0/33 wrong auto-matches
# when the correct job was absent from the pool (BM25 gave 18/33 there — its
# per-query normalisation destroyed the absolute-score floor).
_MATCH_IDF_POWER = float(os.environ.get("LOCAL_MATCH_IDF_POWER", "2.0"))

# v822: pending/running rows older than this are considered stuck (dyno
# restart mid-transcribe) and get re-run when the browser re-uploads.
_STUCK_AFTER_S = 600


def should_reprocess(status, created_at, now=None):
    """True when an existing LocalVideo row should be re-run on re-upload.

    failed -> always (transient ffmpeg/whisper errors were permanent misses).
    pending/running -> only when older than _STUCK_AFTER_S; a live request
    may still be transcribing.  done -> never.
    """
    if status == "failed":
        return True
    if status in ("pending", "running"):
        if not created_at:
            return True
        now = now or datetime.utcnow()
        return (now - created_at).total_seconds() > _STUCK_AFTER_S
    return False


def _extract_audio(src_path: str, wav_path: str) -> tuple:
    """Run ffmpeg, mono 16kHz WAV. Returns (ok, error_msg)."""
    try:
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", src_path, "-vn", "-ac", "1", "-ar", "16000", wav_path],
            check=False, timeout=60,
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            return (False, f"ffmpeg rc={result.returncode}: {(result.stderr or '')[-300:]}")
    except FileNotFoundError:
        return (False, "ffmpeg binary not found on PATH")
    except Exception as e:
        return (False, f"ffmpeg exception: {type(e).__name__}: {str(e)[:120]}")
    return (True, None)


def _transcribe_wav(wav_path: str) -> str:
    segments, _info = _whisper_model().transcribe(wav_path, language=None, beam_size=1)
    return " ".join(seg.text.strip() for seg in segments).strip()


def _bulk_dialogue_map(db, job_ids) -> dict:
    """{job_id: the words actually SPOKEN in the render} for MANY jobs, ONE query.

    THE canonical dialogue builder. Its four consumers (v853 — all of them, at
    last): the IG suggestions endpoint (main.py), the local-folder watcher
    (below), the drive watcher (drive_transcribe) and the IG watcher
    (instagram_transcribe). The last two used to carry a private `_full_dialogue`
    with the OLD text rule and got neither the Prompt-B reworded line nor the
    final cut. The reconstruction rules live in
    instagram_match.reconstruct_dialogue (pure + unit-tested); this function is
    only the DB glue.

    v852: audio_pair rows are now FETCHED (they are the lookup source for their
    visual twin's spoken words — a Prompt-B fallback on the audio twin makes the
    visual's voiceover_line stale). reconstruct_dialogue drops them from the
    OUTPUT, so nothing is double-counted.

    v852 (fix): the final cut is what the EXPORT actually writes, so we also
    fetch Clip.status and the job's clip_order_json lineup. A lineup export
    selects clips by id with NO approval filter (main.py:9232-9241), so the
    lineup IS the cut; without one the cut is completed-and-not-rejected. The
    lineup fetch is ONE bulk query for the whole job pool — never per job (the
    v822.3 N+1 must not come back).

    v822.3: replaces the old per-job `_full_dialogue` N+1. At pool=226 the
    N+1 fired 226 Clip queries PER video, and the v822 rematch sweep ran that
    for ~33 unmatched videos every 30s (~7,500 queries + 7,500 char-level
    SequenceMatchers) synchronously on the web worker → gunicorn WORKER
    TIMEOUT → SIGABRT → killed in-flight DB connections → SSL SYSCALL EOF
    across the whole platform (prod incident 2026-07-06).
    """
    import json
    from collections import defaultdict
    from models import Clip, Job
    import instagram_match as _ig_match
    job_ids = list(job_ids)
    if not job_ids:
        return {}
    rows = (
        db.query(
            Clip.id, Clip.job_id, Clip.clip_index, Clip.clip_role, Clip.paired_clip_id,
            Clip.dialogue_text, Clip.dialogue_text_b, Clip.rendered_prompt_variant,
            Clip.voiceover_line, Clip.approval_status, Clip.status,
        )
        .filter(Clip.job_id.in_(job_ids))
        .order_by(Clip.job_id, Clip.clip_index.asc())
        .all()
    )
    by_job = defaultdict(list)
    for (cid, jid, cidx, role, paired, dt, dtb, variant, vo, approval, status) in rows:
        by_job[jid].append({
            "id": cid,
            "clip_index": cidx,
            "clip_role": role,
            "paired_clip_id": paired,
            "dialogue_text": dt,
            "dialogue_text_b": dtb,
            "rendered_prompt_variant": variant,
            "voiceover_line": vo,
            "approval_status": approval,
            "status": status,
        })

    # ONE bulk query for the lineups of the whole pool (not one per job).
    lineups = {}
    job_rows = (
        db.query(Job.id, Job.clip_order_json)
        .filter(Job.id.in_(job_ids))
        .all()
    )
    for (jid, order_json) in job_rows:
        if not order_json:
            continue
        try:
            ids = json.loads(order_json)
        except (ValueError, TypeError):
            continue  # malformed lineup -> behave as if there were none
        if isinstance(ids, list) and ids:
            lineups[jid] = ids

    return {
        jid: _ig_match.reconstruct_dialogue(clips, lineup_ids=lineups.get(jid))
        for jid, clips in by_job.items()
    }


# ============================================================================
# v856 — MATCH BY FILENAME FIRST. The lookup that cannot be wrong.
#
# The matcher below this comment is an inference engine: it transcribes the
# file, ranks jobs by their words, then probes R2 for a duration and a loudness
# envelope to break the tie. Every step is a comparison with a threshold in it,
# and comparisons can come out wrong — that is what a day of wrong links cost.
#
# But the platform MINTED the file the operator just dropped in, and it named
# it (main.py, v856). The name carries the job id. So before we infer anything,
# we READ it. A stamped name is not evidence to be weighed against other
# evidence; it is the answer, and nothing the transcription finds could improve
# on it.
#
# The inference path stays exactly as it was, for the two cases where the stamp
# is not there: an export minted BEFORE v856 (legacy files still sitting in the
# folder and in R2), and a file the operator RENAMED. Neither is weakened.
# ============================================================================

def resolve_job_by_filename(db, file_name, user_id):
    """The Job whose id the platform stamped into `file_name`, or None.

    Shared by the local watcher and the drive watcher — one definition, because
    two copies of "how do we read our own filename" drift the day the export
    naming changes.

    None means "no answer", NEVER "best answer". Specifically:
      * no stamp in the name (renamed / pre-v856 file) -> None
      * the 8-char prefix resolves to ZERO jobs for this user -> None
      * it resolves to MORE THAN ONE job (an 8-hex-char collision, or another
        user's id colliding inside this user's pool) -> None
    In every one of those cases the caller falls back to the evidence path.
    An ambiguous lookup is not a lookup; we do not break the tie by guessing.
    """
    from models import Job
    import instagram_match as _ig_match

    prefix = _ig_match.job_id_from_filename(file_name)
    if not prefix:
        return None
    # `prefix` is [0-9a-f]{8} by construction, so it carries no LIKE wildcard
    # (% or _) and needs no escaping. Scoped to the owner: a job id belonging to
    # somebody else must never be reachable through a filename.
    hits = (
        db.query(Job)
        .filter(Job.id.like(prefix + "%"), Job.user_id == user_id)
        .all()
    )
    if len(hits) != 1:
        print(f"[filename-match] prefix={prefix} resolved to {len(hits)} jobs "
              f"-> falling back to evidence", flush=True)
        return None
    return hits[0]


# ============================================================================
# v857 — ONE JOB, ONE VIDEO. The exclusivity gate, shared by all three watchers.
#
# evidence_pick is asked "which job produced THIS video" and knows nothing about
# the other videos, so two of them can be handed the SAME job. It happened: reel
# B (0.632s from job J's export length, waveform inconclusive) kept a job that
# reel A proves it owns (0.005s AND a 0.947 waveform), purely because B was
# transcribed first. Nothing in the code even LOOKED.
#
# The DB glue lives here — next to the other cross-watcher glue (_bulk_dialogue_map,
# resolve_job_by_filename) — because three copies of "is somebody already holding
# this job" drift, and the day they drift one watcher starts overwriting links.
# The DECISION itself is pure and unit-tested in instagram_match (claim_strength /
# is_same_video / resolve_claim); this file only reads and writes rows.
#
# THREE WAYS TWO VIDEOS LEGITIMATELY SHARE ONE JOB — none of them a conflict:
#
#   1. A REPOST. The operator posts one export twice (operator-confirmed: two
#      reels, identical duration to the bit, 0.975 waveform to the same job).
#      is_same_video compares the two POSTED files to each other and says so.
#
#   2. THE SAME RENDER ON ANOTHER SURFACE. One job's export is a LocalVideo (the
#      file the operator dropped in the watched folder), a DriveVideo (that same
#      file in Drive) AND an InstagramVideo (the reel posted from it). The IG
#      matcher is DESIGNED to back-fill instagram_url onto a job drive/local
#      watch already published (drive_transcribe: "does NOT touch instagram_url —
#      that's the IG matcher's job"). A cross-surface claim that is WAVEFORM-
#      proven is that same render, not a rival — link it and leave the incumbent
#      alone. (Cross-surface is NOT waved through on a duration-only claim: that
#      is precisely the weak evidence that produced the bad link.)
#
#   3. A stamped filename (v856) — a lookup, not a claim. It does not come
#      through this gate at all.
#
# Everything else is a contest, and the stronger claim wins it.
# ============================================================================

# Which token each watcher stamps on a job it publishes. Reverting a publish is
# only ever OUR business: a job published by drive watch keeps its published
# state when an IG link is removed (mirrors main.py unmatch_video).
_PUBLISHED_VIA = {"instagram": "ig_match", "local": "local_watch", "drive": "drive_watch"}


def _video_label(kind, video):
    """What to call this video in a log line: the handle the operator knows."""
    if kind == "instagram":
        return getattr(video, "shortcode", None) or f"id={getattr(video, 'id', '?')}"
    if kind == "local":
        return getattr(video, "file_name", None) or f"id={getattr(video, 'id', '?')}"
    return getattr(video, "name", None) or f"id={getattr(video, 'id', '?')}"


def find_job_incumbent(db, job_id, kind, video_id):
    """(incumbent_kind, video) — the OTHER video already linked to this job.

    (None, None) when the job is free. Checks all three watcher tables: a job is
    ONE render, and the local watcher holding it does not make it free for a reel
    to claim a second time. SAME-KIND FIRST — a rival reel is the conflict this
    gate exists for; a cross-surface holder is usually the same render (see the
    header), and only matters when there is no rival.

    Job.instagram_video_id is the job's own back-reference and SHOULD agree with
    InstagramVideo.matched_job_id. Both are checked: a disagreement means one
    write half-landed, and a half-linked job must not read as a free one.
    """
    from models import InstagramVideo, LocalVideo, DriveVideo
    tables = [("instagram", InstagramVideo), ("local", LocalVideo), ("drive", DriveVideo)]
    # Look at our own table first, then the others.
    tables.sort(key=lambda t: 0 if t[0] == kind else 1)
    for k, model in tables:
        q = db.query(model).filter(model.matched_job_id == job_id)
        if k == kind:
            q = q.filter(model.id != video_id)
        row = q.first()
        if row is not None:
            return (k, row)

    # The job's own back-reference, in case the reel row lost its half of the link.
    from models import Job
    job = db.query(Job).filter(Job.id == job_id).first()
    back_ref = getattr(job, "instagram_video_id", None) if job is not None else None
    if back_ref and not (kind == "instagram" and back_ref == video_id):
        row = db.query(InstagramVideo).filter(InstagramVideo.id == back_ref).first()
        if row is not None:
            print(f"[claim] job={str(job_id)[:8]} back-reference points at reel "
                  f"{_video_label('instagram', row)} but that reel's matched_job_id "
                  f"does not point back — treating it as the incumbent", flush=True)
            return ("instagram", row)
    return (None, None)


def incumbent_claim_strength(db, job, incumbent):
    """How strongly the video that ALREADY holds this job claims it.

    Recomputed from scratch against this ONE job (a one-candidate pool: we are
    not asking "which job is the incumbent's", we are asking "how well does the
    incumbent explain THIS job"), so the two claims are scored by the same ruler.
    """
    import instagram_match as _ig_match
    cand = [{
        "job_id": job.id,
        "export_duration_s": job.export_duration_s,
        "export_audio_fp": job.export_audio_fp,
    }]
    ev = _ig_match.evidence_pick(
        getattr(incumbent, "duration_s", None),
        getattr(incumbent, "audio_fp", None),
        cand,
    )
    return _ig_match.claim_strength(ev)


def release_incumbent(db, job, inc_kind, incumbent):
    """Unlink the incumbent from this job and undo the publish IT caused.

    Mirrors main.py unmatch_video / diag_apply_relinks: clearing the link alone
    leaves the job parked in `published`, so a wrong match was unrecoverable and
    an unfinished job sat at the end of the board. Only revert when the publish
    was OURS to revert — published_via names the watcher that did it, and NULL
    means the IG match did (drive/local always stamp their own token).

    Does not commit: the caller writes the new link in the same transaction, so
    the job is never momentarily owned by nobody.
    """
    from models import Clip
    from lifecycle import derive_effective_stage

    incumbent.matched_job_id = None
    incumbent.matched_at = None
    if hasattr(incumbent, "match_score"):
        incumbent.match_score = None
    # The link is gone, so its provenance goes with it — a stale 'manual' left on
    # an unlinked row would make the NEXT link look hand-made and unstealable.
    if hasattr(incumbent, "match_source"):
        incumbent.match_source = None
    if inc_kind == "instagram" and job.instagram_video_id == incumbent.id:
        job.instagram_url = None
        job.instagram_video_id = None

    reverted_to = None
    via = job.published_via or ("ig_match" if inc_kind == "instagram" else "")
    if via == _PUBLISHED_VIA[inc_kind]:
        approved = db.query(Clip).filter(
            Clip.job_id == job.id, Clip.approval_status == "approved",
        ).count()
        job.lifecycle_stage = None   # re-derive from live state, not the stale column
        job.lifecycle_stage = derive_effective_stage(job, approved)
        job.published_via = None
        job.published_at = None
        reverted_to = job.lifecycle_stage
    return reverted_to


def release_other_holders(db, job, video, kind="instagram"):
    """Unlink every OTHER video of this kind that is already holding `job`.

    A job produced ONE video, and there is no unique constraint saying so — the
    only thing stopping two rows from pointing at one job is the code that writes
    them. An OPERATOR pick (main.py match_video) is the one link that skips the
    evidence gate, so it needs its own release: without it the popover's promise
    ("already linked to X — picking this moves the link") is a lie, both rows keep
    matched_job_id, and the phantom holder poisons find_job_incumbent (a .first()
    with no order_by picks whichever row the DB hands back).

    The repost is left alone — one export posted twice, both reels legitimately
    hold the job (is_same_video compares the two POSTED files to each other).
    Cross-surface holders are left alone too: the export FILE and the reel posted
    from it are the same render on two surfaces, not rivals (see the header).

    Does not commit — the caller writes the new link in the same transaction.
    Returns the labels of the videos it released.
    """
    from models import InstagramVideo, LocalVideo, DriveVideo
    import instagram_match as _ig_match
    model = {"instagram": InstagramVideo, "local": LocalVideo, "drive": DriveVideo}[kind]

    released = []
    holders = (
        db.query(model)
        .filter(model.matched_job_id == job.id, model.id != video.id)
        .all()
    )
    for other in holders:
        them = _video_label(kind, other)
        if _ig_match.is_same_video(
            getattr(video, "audio_fp", None), getattr(other, "audio_fp", None),
            getattr(video, "duration_s", None), getattr(other, "duration_s", None),
        ):
            print(f"[claim] job={str(job.id)[:8]} {kind}={_video_label(kind, video)} "
                  f"LINK alongside {kind}={them} repost=true", flush=True)
            continue
        reverted_to = release_incumbent(db, job, kind, other)
        released.append(them)
        print(f"[claim] job={str(job.id)[:8]} {kind}={_video_label(kind, video)} "
              f"RELEASED {kind}={them} reason=manual-pick "
              f"incumbent_reverted_to={reverted_to}", flush=True)
    return released


def enforce_exclusivity(db, job, video, kind, evidence):
    """The gate every auto-link passes through: "link" | "steal" | "refuse".

    link   — the job is free, or the two videos are the same render (a repost, or
             the export and the reel made from it). The incumbent is left alone.
    steal   — the challenger's claim is clearly stronger: the incumbent is
             UNLINKED here (and its publish reverted) so the caller can link.
    refuse — do not link. Left for a manual pick.

    Never raises and never silently overwrites: on any internal error it refuses,
    because not linking is the recoverable outcome. Every branch logs one line.
    """
    import instagram_match as _ig_match
    try:
        inc_kind, inc = find_job_incumbent(db, job.id, kind, video.id)
        if inc is None:
            return "link"

        me = _video_label(kind, video)
        them = _video_label(inc_kind, inc)
        repost = _ig_match.is_same_video(
            getattr(video, "audio_fp", None), getattr(inc, "audio_fp", None),
            getattr(video, "duration_s", None), getattr(inc, "duration_s", None),
        )
        # A waveform-proven claim from ANOTHER surface is the same render seen
        # somewhere else (the export file vs the reel posted from it), not a rival.
        cross_surface = (
            inc_kind != kind
            and "waveform" in str((evidence or {}).get("source") or "")
        )
        if repost or cross_surface:
            print(f"[claim] job={str(job.id)[:8]} {kind}={me} LINK alongside "
                  f"{inc_kind}={them} repost={str(bool(repost)).lower()} "
                  f"cross_surface={str(bool(cross_surface)).lower()}", flush=True)
            return "link"

        # A HUMAN'S LINK IS NOT A CLAIM. The strength below is computed from MEDIA
        # EVIDENCE, and an operator's manual pick carries none — it scores ~0, so
        # any waveform-clearing challenger could evict it. The displaced reel is
        # then never re-matched (transcribe_one is idempotent on 'done'), so the
        # operator's repair would be DESTROYED, not moved. Same for a filename
        # stamp (v856): the platform minted that name, it is a lookup, not a claim.
        #
        # A NULL match_source on an EXISTING row is a LEGACY link, made before this
        # column existed. We cannot prove it was machine-made, and destroying a
        # possibly hand-made link is far worse than declining a steal — so NULL is
        # read as 'manual'. Only a link this code stamped 'evidence' can be taken.
        inc_source = getattr(inc, "match_source", None) or "manual"
        if inc_source in ("manual", "filename"):
            print(f"[claim] job={str(job.id)[:8]} {kind}={me} REFUSE "
                  f"reason=incumbent-is-{inc_source} incumbent={them}", flush=True)
            return "refuse"

        challenger = _ig_match.claim_strength(evidence)
        holder = incumbent_claim_strength(db, job, inc)
        verdict = _ig_match.resolve_claim(challenger, holder, False)
        if verdict == "steal":
            reverted_to = release_incumbent(db, job, inc_kind, inc)
            print(f"[claim] job={str(job.id)[:8]} {kind}={me} STEAL from "
                  f"{inc_kind}={them} challenger={challenger:.3f} incumbent={holder:.3f} "
                  f"margin={_ig_match.CLAIM_MARGIN} incumbent_reverted_to={reverted_to}",
                  flush=True)
            return "steal"
        print(f"[claim] job={str(job.id)[:8]} {kind}={me} REFUSE "
              f"reason=weaker-claim incumbent={them} challenger={challenger:.3f} "
              f"incumbent_strength={holder:.3f}", flush=True)
        return "refuse"
    except Exception as e:
        # Refusing is the safe failure: the video waits for a manual pick. Linking
        # through a broken gate is how a wrong link gets written.
        #
        # ROLL BACK FIRST. release_incumbent clears the incumbent's link BEFORE it
        # re-derives the stage, so a failure in derive_effective_stage (or the Clip
        # count) leaves the link DESTROYED and never replaced. The local sweep runs
        # many videos on the REQUEST session, so the next video's db.commit() would
        # flush that hole to disk. Returning "refuse" on a half-applied release is
        # not a refusal, it is data loss.
        try:
            db.rollback()
        except Exception as re:   # a fake/closed session must not mask the refusal
            print(f"[claim] rollback after gate-error failed: "
                  f"{type(re).__name__}: {str(re)[:100]}", flush=True)
        print(f"[claim] job={str(getattr(job, 'id', '?'))[:8]} REFUSE reason=gate-error "
              f"{type(e).__name__}: {str(e)[:160]}", flush=True)
        return "refuse"


def _advance_job_to_published(video, job, score, db, match_source="evidence") -> None:
    """Mark a LocalVideo matched + advance its Job to published/local_watch.

    published_via is only claimed when NOBODY ELSE published this job. A local
    file can legitimately link ALONGSIDE a reel (the export and the reel posted
    from it are the same render), and that job is already `published` via
    'ig_match' — overwriting the token with 'local_watch' then makes
    main.py unmatch_video refuse to revert its own publish (it only reverts
    'ig_match', or NULL, which reads as ig_match). Provenance belongs to whoever
    did the publishing; we only take it when we ARE that one.
    """
    video.matched_job_id = job.id
    video.matched_at = datetime.utcnow()
    video.match_score = score
    video.match_source = match_source
    if job.lifecycle_stage != "published" and not job.published_via:
        job.published_via = "local_watch"
    job.lifecycle_stage = "published"
    if job.published_at is None:
        job.published_at = datetime.utcnow()
    db.commit()
    print(f"[local] AUTO-MATCH hash={video.file_hash[:8]} -> job={str(job.id)[:8]} "
          f"score={score:.3f} match_source={match_source}", flush=True)


def _maybe_auto_match(video, db: Session, candidates=None, dialogue_map=None,
                      budget_s=None) -> None:
    """TEXT RANKS, EVIDENCE DECIDES (v855). On proven evidence, advance.

    The uploaded file IS the export — not a re-encoded reel — so the media
    evidence here is the strongest we ever get: same bytes, same loudness
    envelope, same runtime as the job's mp4 in R2.

    It used to auto-publish on the TF-IDF text ranking alone. Builds share their
    script VERBATIM, so the text regularly scored two candidates identically and
    the "winner" was a coin flip. No evidence now means NO auto-publish: the
    video stays unmatched for a manual pick.

    candidates / dialogue_map may be supplied by the sweep so a batch of videos
    shares ONE candidate load + ONE bulk-dialogue query (v822.3).

    budget_s: wall-clock seconds this call may spend probing R2. The sweep passes
    its REMAINING budget so a single video's probes cannot overrun the sweep's
    guard (which is only checked BETWEEN videos). None -> the export_probe
    default.
    """
    try:
        from models import Job
        import instagram_match as _ig_match
        from export_probe import evidence_candidates, LAZY_PROBE_BUDGET_S

        # v856 — the filename first. This runs BEFORE the transcription gate on
        # purpose: a stamped name identifies the job even when Whisper produced
        # nothing (a silent export still has a name), and no amount of text or
        # waveform evidence can beat an id we wrote ourselves.
        stamped = resolve_job_by_filename(db, video.file_name, video.user_id)
        if stamped is not None:
            print(
                f"[local] hash={video.file_hash[:8]} decision=AUTO source=filename "
                f"job={str(stamped.id)[:8]} file={video.file_name}",
                flush=True,
            )
            # score=1.0: this is not a similarity, it is a certainty.
            _advance_job_to_published(video, stamped, 1.0, db, match_source="filename")
            return

        if not video.transcription:
            return
        if candidates is None:
            candidates = (
                db.query(Job)
                .filter(
                    Job.user_id == video.user_id,
                    Job.lifecycle_stage == "awaiting_finishing",
                )
                .all()
            )
        # v855 recency window. A LocalVideo has no posted_at — it is uploaded the
        # moment the operator finishes it, so its own created_at IS the post time.
        ref_at = video.created_at or datetime.utcnow()
        candidates = [
            j for j in candidates
            if _ig_match.within_recency_window(j.created_at, ref_at)
        ]
        if not candidates:
            print(f"[local] hash={video.file_hash[:8]} decision=MANUAL source=none "
                  f"reason=empty-pool pool=0", flush=True)
            return
        if dialogue_map is None:
            dialogue_map = _bulk_dialogue_map(db, [j.id for j in candidates])

        cand_pairs = [(j.id, dialogue_map.get(j.id, "")) for j in candidates]
        ranked = _ig_match.rank_tfidf(video.transcription, cand_pairs, idf_power=_MATCH_IDF_POWER)
        # Text only names the shortlist worth probing from R2 (a brand-new job has
        # no cached duration/fingerprint yet). It does not get a vote.
        shortlist = [r["job_id"] for r in ranked[:6]]
        cands = evidence_candidates(
            db, candidates, priority_ids=shortlist,
            budget_s=LAZY_PROBE_BUDGET_S if budget_s is None else budget_s,
        )
        ev = _ig_match.evidence_pick(video.duration_s, video.audio_fp, cands)

        pick_id = ev["job_id"]
        text_top = ranked[0]["job_id"] if ranked else None
        reason = (
            "conflict" if ev["conflict"]
            else ("" if pick_id else ("no-file-fp" if not video.audio_fp else "no-evidence"))
        )
        print(
            f"[local] hash={video.file_hash[:8]} "
            f"decision={'AUTO' if pick_id else 'MANUAL'} "
            f"source={ev['source'] or 'none'} job={str(pick_id or '-')[:8]} "
            f"sim={ev['similarity']} dur_delta={ev['dur_delta']} pool={len(candidates)} "
            f"file_dur={video.duration_s} text_top={str(text_top or '-')[:8]}"
            + (f" reason={reason}" if reason else ""),
            flush=True,
        )
        if not pick_id:
            return
        job = next((j for j in candidates if j.id == pick_id), None)
        if job is None:
            job = db.query(Job).filter_by(id=pick_id).first()
        if not job:
            return
        # match_score records the confidence we ACTUALLY decided on: the waveform
        # similarity when the waveform decided, else the text score of the job the
        # duration proved (so the column never reads 0.0 on a proven match).
        # `not ev["similarity"]`, NOT `is None`: when the DURATION decides,
        # evidence_pick can hand back a similarity of exactly 0.0 (silent audio,
        # no overlap) — and 0.0 is not None, so an `is None` test stored it and
        # the UI rendered "0%" on a match the media PROVED.
        text_score = next((r["score"] for r in ranked if r["job_id"] == pick_id), 0.0)
        # v857 — a job produced ONE video. If another video already holds this
        # one, the stronger claim takes it; a weaker claim is refused, never
        # written over the top.
        if enforce_exclusivity(db, job, video, "local", ev) == "refuse":
            return
        _advance_job_to_published(video, job, ev["similarity"] or text_score, db,
                                  match_source="evidence")
    except Exception as exc:
        print(f"[local] auto-match error on hash={video.file_hash[:8]}: {type(exc).__name__}: {str(exc)[:200]}", flush=True)


# v822.3 sweep guards — the rematch sweep used to re-score EVERY unmatched
# local video against the WHOLE awaiting_finishing pool every 30s. That is an
# unbounded O(videos × pool) job on the web worker; at 33×226 it SIGABRT'd the
# platform. Bounds: recent videos only, capped count, per-user cooldown, hard
# wall-clock budget. Old orphans (created before the age window) are genuine
# non-matches — the manual ✕ / Force rescan is the escape hatch for those.
_SWEEP_COOLDOWN_S = 20
_SWEEP_BUDGET_S = 6.0
_SWEEP_MAX_VIDEOS = 25
_SWEEP_MAX_AGE_H = 48
_last_sweep_at = {}  # user_id -> monotonic timestamp (per web-worker process)


def rematch_unmatched(user_id, db: Session) -> dict:
    """Re-score RECENT done-but-unmatched LocalVideos against the CURRENT
    awaiting_finishing pool.  Called by the browser after each scan.

    Closes the race where a video was uploaded before its job reached
    Finishing (match ran once, at upload time).  Bounded per v822.3 so it can
    never again saturate the web worker.
    """
    import time as _time
    from datetime import timedelta
    from models import LocalVideo, Job

    now_m = _time.monotonic()
    last = _last_sweep_at.get(user_id, 0.0)
    if now_m - last < _SWEEP_COOLDOWN_S:
        return {"checked": 0, "matched": 0, "skipped": "cooldown"}
    _last_sweep_at[user_id] = now_m

    candidates = (
        db.query(Job)
        .filter(Job.user_id == user_id, Job.lifecycle_stage == "awaiting_finishing")
        .all()
    )
    if not candidates:
        return {"checked": 0, "matched": 0}

    cutoff = datetime.utcnow() - timedelta(hours=_SWEEP_MAX_AGE_H)
    vids = (
        db.query(LocalVideo)
        .filter(
            LocalVideo.user_id == user_id,
            LocalVideo.transcription_status == "done",
            LocalVideo.matched_job_id == None,  # noqa: E711
            LocalVideo.created_at >= cutoff,
        )
        .order_by(LocalVideo.created_at.desc())
        .limit(_SWEEP_MAX_VIDEOS)
        .all()
    )
    if not vids:
        return {"checked": 0, "matched": 0}

    # ONE bulk dialogue query for the whole sweep (shared across all videos).
    dialogue_map = _bulk_dialogue_map(db, [j.id for j in candidates])
    _t0 = _time.monotonic()
    matched = 0
    checked = 0
    for v in vids:
        remaining = _SWEEP_BUDGET_S - (_time.monotonic() - _t0)
        if remaining <= 0:
            print(f"[local] rematch sweep budget hit after {checked}/{len(vids)} videos", flush=True)
            break
        checked += 1
        # Hand the REMAINING budget down. This check only fires BETWEEN videos,
        # so without it one video's R2 probes (download + ffmpeg each) could
        # overrun the whole sweep's guard by minutes — and this sweep runs
        # unattended on every browser poll cycle.
        _maybe_auto_match(v, db, candidates=candidates, dialogue_map=dialogue_map,
                          budget_s=remaining)
        if v.matched_job_id:
            matched += 1
            mjid = v.matched_job_id
            candidates = [j for j in candidates if j.id != mjid]
            dialogue_map.pop(mjid, None)
    print(
        f"[local] rematch sweep user={str(user_id)[:8]} checked={checked}/{len(vids)} "
        f"pool={len(candidates)} matched={matched} dur={_time.monotonic() - _t0:.1f}s",
        flush=True,
    )
    return {"checked": checked, "matched": matched}


def transcribe_local(video, file_bytes: bytes, db: Session) -> None:
    """Process an uploaded LocalVideo: ffmpeg + Whisper + match + advance.

    Caller must have already created the LocalVideo row with status='pending'.
    file_bytes is the raw upload payload (we already deduped by hash upstream).
    """
    if video.transcription_status == "done":
        return
    video.transcription_status = "running"
    db.commit()
    try:
        with tempfile.TemporaryDirectory() as tmp:
            ext = os.path.splitext(video.file_name)[1].lower() or ".mp4"
            src = os.path.join(tmp, f"upload{ext}")
            with open(src, "wb") as f:
                f.write(file_bytes)
            wav = os.path.join(tmp, "audio.wav")
            ok, err = _extract_audio(src, wav)
            if not ok:
                video.transcription_status = "failed"
                video.transcription_error = (err or "ffmpeg failed")[:500]
                db.commit()
                return
            # v855 — the uploaded file IS the export. Take its runtime + loudness
            # envelope now, while it is on disk: this is the evidence the matcher
            # decides on, and the bytes are gone when this tempdir closes.
            _fingerprint_media(video, src, db)
            print(f"[local] hash={video.file_hash[:8]} starting Whisper", flush=True)
            text = _transcribe_wav(wav)
            print(f"[local] hash={video.file_hash[:8]} Whisper done ({len(text)}c)", flush=True)
        video.transcription = text
        video.transcription_status = "done"
        video.transcription_error = None
        db.commit()
        _maybe_auto_match(video, db)
    except Exception as exc:
        video.transcription_status = "failed"
        video.transcription_error = str(exc)[:500]
        db.commit()
