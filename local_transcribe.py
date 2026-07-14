# -*- coding: utf-8 -*-
"""Local folder uploads → transcribe → match → advance.

Browser-based watcher (File System Access API) detects new files in a
user-picked folder + POSTs each as multipart to /api/local-videos/upload.
This module handles the server-side pipeline:
  1. Save the uploaded blob to a tempfile.
  2. ffmpeg → mono 16k WAV.
  3. faster-whisper → transcript.
  4. Score against the user's awaiting_finishing jobs.
  5. On match >= IG_AUTO_MATCH_THRESHOLD, advance the job to published with
     published_via='local_watch'.

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
from instagram_transcribe import _model as _whisper_model

_AUTO_MATCH_THRESHOLD = float(os.environ.get("IG_AUTO_MATCH_THRESHOLD", "0.70"))

# v822.4 LOCAL matcher (TF-IDF cosine + margin gate). Env-tunable WITHOUT a
# deploy so the operator can calibrate from the `[local] ... s1=/s2=/margin=`
# log lines. HIGH = min cosine for the top candidate; MARGIN = how far the top
# must beat the runner-up to auto-publish (near-duplicate twins sit within
# MARGIN -> deferred to manual pick, never auto-matched to the wrong one).
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

    THE canonical dialogue builder — feeds the IG suggestions endpoint, the
    drive watcher, and the local-folder watcher. The reconstruction rules live
    in instagram_match.reconstruct_dialogue (pure + unit-tested); this function
    is only the DB glue.

    v852: audio_pair rows are now FETCHED (they are the lookup source for their
    visual twin's spoken words — a Prompt-B fallback on the audio twin makes the
    visual's voiceover_line stale). reconstruct_dialogue drops them from the
    OUTPUT, so nothing is double-counted.

    v822.3: replaces the old per-job `_full_dialogue` N+1. At pool=226 the
    N+1 fired 226 Clip queries PER video, and the v822 rematch sweep ran that
    for ~33 unmatched videos every 30s (~7,500 queries + 7,500 char-level
    SequenceMatchers) synchronously on the web worker → gunicorn WORKER
    TIMEOUT → SIGABRT → killed in-flight DB connections → SSL SYSCALL EOF
    across the whole platform (prod incident 2026-07-06).
    """
    from collections import defaultdict
    from models import Clip
    import instagram_match as _ig_match
    job_ids = list(job_ids)
    if not job_ids:
        return {}
    rows = (
        db.query(
            Clip.id, Clip.job_id, Clip.clip_index, Clip.clip_role, Clip.paired_clip_id,
            Clip.dialogue_text, Clip.dialogue_text_b, Clip.rendered_prompt_variant,
            Clip.voiceover_line, Clip.approval_status,
        )
        .filter(Clip.job_id.in_(job_ids))
        .order_by(Clip.job_id, Clip.clip_index.asc())
        .all()
    )
    by_job = defaultdict(list)
    for (cid, jid, cidx, role, paired, dt, dtb, variant, vo, approval) in rows:
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
        })
    return {jid: _ig_match.reconstruct_dialogue(clips) for jid, clips in by_job.items()}


def _advance_job_to_published(video, job, score, db) -> None:
    """Mark a LocalVideo matched + advance its Job to published/local_watch."""
    video.matched_job_id = job.id
    video.matched_at = datetime.utcnow()
    video.match_score = score
    job.lifecycle_stage = "published"
    job.published_via = "local_watch"
    if job.published_at is None:
        job.published_at = datetime.utcnow()
    db.commit()
    print(f"[local] AUTO-MATCH hash={video.file_hash[:8]} -> job={str(job.id)[:8]} score={score:.3f}", flush=True)


def _maybe_auto_match(video, db: Session, candidates=None, dialogue_map=None) -> None:
    """Score the transcript against awaiting_finishing jobs. On match, advance.

    candidates / dialogue_map may be supplied by the sweep so a batch of
    videos shares ONE candidate load + ONE bulk-dialogue query (v822.3).
    When omitted (upload path) they are built here — still ONE bulk query,
    never the old per-job N+1.
    """
    try:
        from models import Job
        import instagram_match as _ig_match
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
        if not candidates:
            print(f"[local] hash={video.file_hash[:8]} no awaiting_finishing candidates", flush=True)
            return
        if dialogue_map is None:
            dialogue_map = _bulk_dialogue_map(db, [j.id for j in candidates])

        # v822.4: TF-IDF cosine + margin gate. The old char-level score()
        # matched near-duplicate scripts (shared ED body) as the WRONG twin
        # at up to 1.000. TF-IDF down-weights the shared boilerplate; the
        # margin gate auto-matches ONLY when the top candidate clearly beats
        # the runner-up. Ambiguous (near-duplicate) -> NO auto-publish; the
        # video stays "no match" for a manual pick instead of a wrong link.
        cand_pairs = [(j.id, dialogue_map.get(j.id, "")) for j in candidates]
        ranked = _ig_match.rank_tfidf(video.transcription, cand_pairs, idf_power=_MATCH_IDF_POWER)
        if not ranked:
            print(f"[local] hash={video.file_hash[:8]} no ranking | pool={len(candidates)} tlen={len(video.transcription or '')}", flush=True)
            return
        s1 = ranked[0]["score"]
        s2 = ranked[1]["score"] if len(ranked) > 1 else 0.0
        pick_id = _ig_match.auto_pick(ranked, _MATCH_HIGH, _MATCH_MARGIN)
        print(
            f"[local] hash={video.file_hash[:8]} top_job={str(ranked[0]['job_id'])[:8]} "
            f"s1={s1:.3f} s2={s2:.3f} margin={s1 - s2:.3f} "
            f"decision={'AUTO' if pick_id else 'MANUAL'} pool={len(candidates)} "
            f"tlen={len(video.transcription or '')}",
            flush=True,
        )
        if not pick_id:
            # Ambiguous or low-confidence -> leave unmatched for manual pick.
            return
        job = next((j for j in candidates if j.id == pick_id), None)
        if job is None:
            job = db.query(Job).filter_by(id=pick_id).first()
        if not job:
            return
        _advance_job_to_published(video, job, s1, db)
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
        if _time.monotonic() - _t0 > _SWEEP_BUDGET_S:
            print(f"[local] rematch sweep budget hit after {checked}/{len(vids)} videos", flush=True)
            break
        checked += 1
        _maybe_auto_match(v, db, candidates=candidates, dialogue_map=dialogue_map)
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
