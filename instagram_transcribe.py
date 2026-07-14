"""Background transcription pass for pending InstagramVideo rows.

Run inside the existing worker loop (`code/worker.py`). Picks pending
videos one at a time, downloads the direct HikerAPI video_url via
requests (no yt-dlp — auth-free signed URLs), extracts audio via
ffmpeg, transcribes via faster-whisper (int8 CPU), writes back.

If the stored video_url is missing or expired (HikerAPI URLs are signed
and time-limited), re-fetches a fresh URL via HikerAPI before failing.
"""
import os
import subprocess
import tempfile
from datetime import datetime
from typing import Optional
import requests
from sqlalchemy.orm import Session

# Cached model instance. Loaded once per process.
_WHISPER_MODEL = None
_WHISPER_MODEL_NAME = os.environ.get("WHISPER_MODEL", "base")


def _model():
    global _WHISPER_MODEL
    if _WHISPER_MODEL is None:
        from faster_whisper import WhisperModel
        _WHISPER_MODEL = WhisperModel(_WHISPER_MODEL_NAME, device="cpu", compute_type="int8")
    return _WHISPER_MODEL


def _earliest_awaiting_finishing_approval(db: Session, user_id: str) -> Optional[datetime]:
    """Earliest approval_at across this user's COMPLETED, unlinked Jobs.
    IG videos posted before this time cannot possibly match any candidate.

    Mirrors the matcher candidate pool (status=='completed' + unlinked) —
    NOT lifecycle_stage, which is derived live + persisted lazily and would
    skip-transcribe videos whose source job is stuck at awaiting_export.
    Returns None (cutoff disabled) if the earliest job has no approval_at,
    which safely transcribes everything rather than wrongly skipping."""
    from models import Job
    row = (
        db.query(Job.approval_at)
        .filter(
            Job.user_id == user_id,
            Job.status == "completed",
            Job.instagram_video_id.is_(None),
            Job.archived == False,  # noqa: E712
        )
        .order_by(Job.approval_at.asc())
        .first()
    )
    return row[0] if row and row[0] else None


def _fetch_fresh_video_url(video, account) -> Optional[str]:
    """Re-pull the clip from HikerAPI to get a non-expired video_url.
    Costs 1 HikerAPI call ($0.001). Used as fallback when stored URL 403s."""
    try:
        from encryption import decrypt as _enc_decrypt
        from instagram_client import fetch_recent_clips
        api_key = _enc_decrypt(account.api_key_encrypted)
        if not account.ig_user_id:
            return None
        # Pull a generous window; older reels may need many pages.
        clips = fetch_recent_clips(account.ig_user_id, api_key, limit=200, max_pages=10)
        for c in clips:
            if c.get("shortcode") == video.shortcode:
                return c.get("video_url")
    except Exception:
        return None
    return None


def _download_and_extract_audio(direct_video_url: str, work_dir: str) -> tuple:
    """Downloads the direct video URL + extracts mono 16k WAV.

    direct_video_url MUST be a fully-signed HikerAPI/fbcdn URL (not the
    /reel/ permalink — that requires auth). Returns (wav_path, mp4_path,
    error_msg); wav_path is None on failure and error_msg explains why.

    v855 — the mp4 path comes back too: the matcher needs the reel's OWN media
    evidence (loudness fingerprint + true runtime), and here is the one moment
    the file is on disk. IG video_urls expire, so re-downloading it later is not
    a given.
    """
    if not direct_video_url:
        return (None, None, "no video_url in DB")
    mp4 = os.path.join(work_dir, "ig_video.mp4")
    try:
        with requests.get(direct_video_url, stream=True, timeout=60) as r:
            if r.status_code != 200:
                return (None, None, f"download HTTP {r.status_code}")
            ctype = r.headers.get("content-type", "")
            with open(mp4, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)
        size = os.path.getsize(mp4) if os.path.exists(mp4) else 0
        print(f"[ig-transcribe] downloaded {size}B content-type={ctype}", flush=True)
    except Exception as e:
        return (None, None, f"download exception: {type(e).__name__}: {str(e)[:120]}")
    if not os.path.exists(mp4) or os.path.getsize(mp4) < 1024:
        return (None, None, f"downloaded file too small ({os.path.getsize(mp4) if os.path.exists(mp4) else 0}B)")
    wav = os.path.join(work_dir, "ig_audio.wav")
    try:
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", mp4, "-vn", "-ac", "1", "-ar", "16000", wav],
            check=False, timeout=30,
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            stderr_tail = (result.stderr or "")[-300:]
            return (None, mp4, f"ffmpeg rc={result.returncode}: {stderr_tail}")
    except FileNotFoundError:
        return (None, mp4, "ffmpeg binary not found on PATH")
    except Exception as e:
        return (None, mp4, f"ffmpeg exception: {type(e).__name__}: {str(e)[:120]}")
    return (wav, mp4, None)


def fingerprint_downloaded_media(video, media_path, db) -> None:
    """v855 — stamp a video row with the media evidence of the file we just got.

    Shared by all three watchers (IG / local / drive): the posted reel or the
    uploaded export is the ONLY copy of the render we can measure, and it is on
    disk exactly once — during transcription.

    A fingerprint failure must NEVER break transcription: the transcript is still
    useful, the match just falls back to a manual pick. audio_fp_at is stamped on
    every attempt (success or not) so a dead url is not retried forever; audio_fp
    itself is only written on success, so "" never masquerades as evidence.
    """
    try:
        from audio_fingerprint import fingerprint_media
        from export_probe import probe_duration
        if not media_path or not os.path.exists(media_path):
            return
        if not video.audio_fp:
            blob = fingerprint_media(media_path)
            if blob:
                video.audio_fp = blob
            video.audio_fp_at = datetime.utcnow()
        if video.duration_s is None:
            dur = probe_duration(media_path)
            if dur is not None:
                video.duration_s = dur
        db.commit()
        print(
            f"[media-fp] video={getattr(video, 'shortcode', None) or getattr(video, 'id', '?')} "
            f"fp={'yes' if video.audio_fp else 'no'} dur={video.duration_s}",
            flush=True,
        )
    except Exception as e:
        print(f"[media-fp] fingerprint failed: {type(e).__name__}: {str(e)[:160]}", flush=True)


def _maybe_auto_match(video, account, db: Session) -> None:
    """After transcription: TEXT RANKS, EVIDENCE DECIDES.

    v855 — this used to auto-publish on the TEXT ranking alone (rank_tfidf +
    the HIGH/MARGIN gate). That is what produced the wrong links we spent a day
    undoing: ~787 jobs, many sharing a script VERBATIM — on the 14 disputed
    reels the top-1 and top-2 scores came out EXACTLY equal, so the "margin"
    the gate trusted was noise.

    The words now only NARROW the field. The decision is made by the MEDIA:
    the export's runtime and its loudness envelope, compared against the reel's
    (see instagram_match.evidence_pick — thresholds measured on production).

      * evidence decides -> auto-match THAT job, even if the text disagreed.
        The media is the ground truth; the text is a hint.
      * evidence does not decide -> DO NOT AUTO-PUBLISH. It goes to the manual
        suggestions popover. Abstaining is correct behavior — an unlinked reel
        costs one click, a wrongly-published one costs a day.
    """
    try:
        from models import Job
        import instagram_match as _ig_match
        from export_probe import evidence_candidates
        # Imported INSIDE the function on purpose: local_transcribe imports this
        # module for the cached Whisper model, so a module-scope import here
        # would close the cycle.
        from local_transcribe import _bulk_dialogue_map, _MATCH_IDF_POWER
        if not video.transcription:
            return
        # Candidate pool = any COMPLETED, unlinked, non-archived job. Not
        # gated on lifecycle_stage — that column is derived live + persisted
        # lazily, so exported b-roll/twin jobs stuck at awaiting_export get
        # silently dropped. status=='completed' is the durable signal.
        candidates = (
            db.query(Job)
            .filter(
                Job.user_id == account.user_id,
                Job.status == "completed",
                Job.instagram_video_id.is_(None),
                Job.archived == False,  # noqa: E712
            )
            .all()
        )
        # v855 RECENCY WINDOW — a job created after the reel was posted cannot be
        # its source, and one built a season earlier is not what the operator just
        # posted either (measured max job age at post time: 20.99 days). 30 days
        # is that with headroom. A smaller pool is also a cheaper waveform search.
        candidates = [
            j for j in candidates
            if _ig_match.within_recency_window(j.created_at, video.posted_at)
        ]
        if not candidates:
            print(f"[ig-auto] shortcode={video.shortcode} decision=MANUAL source=none "
                  f"reason=empty-pool pool=0", flush=True)
            return

        dialogue_map = _bulk_dialogue_map(db, [j.id for j in candidates])
        cand_pairs = [(j.id, dialogue_map.get(j.id, "")) for j in candidates]
        ranked = _ig_match.rank_tfidf(
            video.transcription, cand_pairs, idf_power=_MATCH_IDF_POWER,
        )
        # The text ranking's only job now: name the shortlist worth PROBING. A
        # brand-new job has no cached duration/fingerprint, and probing every
        # candidate would download the whole pool from R2.
        shortlist = [r["job_id"] for r in ranked[:6]]
        cands = evidence_candidates(db, candidates, priority_ids=shortlist)
        ev = _ig_match.evidence_pick(video.duration_s, video.audio_fp, cands)

        pick_id = ev["job_id"]
        reason = (
            "conflict" if ev["conflict"]
            else ("" if pick_id else ("no-reel-fp" if not video.audio_fp else "no-evidence"))
        )
        text_top = ranked[0]["job_id"] if ranked else None
        print(
            f"[ig-auto] shortcode={video.shortcode} "
            f"decision={'AUTO' if pick_id else 'MANUAL'} "
            f"source={ev['source'] or 'none'} job={str(pick_id or '-')[:8]} "
            f"sim={ev['similarity']} dur_delta={ev['dur_delta']} pool={len(candidates)} "
            f"reel_dur={video.duration_s} text_top={str(text_top or '-')[:8]} "
            f"text_agrees={pick_id == text_top if pick_id else None}"
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
        video.matched_job_id = job.id
        video.matched_at = datetime.utcnow()
        job.instagram_url = video.url
        job.instagram_video_id = video.id
        job.lifecycle_stage = "published"
        if job.published_at is None:
            job.published_at = datetime.utcnow()
        db.commit()
        print(f"[ig-auto] AUTO-MATCH shortcode={video.shortcode} -> job={str(job.id)[:8]} "
              f"via={ev['source']} sim={ev['similarity']} dur_delta={ev['dur_delta']}", flush=True)
    except Exception as exc:
        print(f"[ig-auto] auto-match error on shortcode={video.shortcode}: {type(exc).__name__}: {str(exc)[:200]}", flush=True)


def _transcribe_audio(wav_path: str) -> str:
    segments, _info = _model().transcribe(wav_path, language=None, beam_size=1)
    return " ".join(seg.text.strip() for seg in segments).strip()


def transcribe_one(video, db: Session) -> None:
    """Transcribes a single InstagramVideo row in place. Idempotent on `done`."""
    from models import InstagramVideo, InstagramAccount
    if video.transcription_status == "done":
        return
    video.transcription_status = "running"
    db.commit()
    try:
        account = db.query(InstagramAccount).filter_by(id=video.account_id).first()
        if not account:
            video.transcription_status = "failed"
            video.transcription_error = "owning account missing"
            db.commit()
            return
        cutoff = _earliest_awaiting_finishing_approval(db, account.user_id)
        if cutoff and video.posted_at and video.posted_at < cutoff:
            video.transcription_status = "skipped"
            db.commit()
            return
        with tempfile.TemporaryDirectory() as tmp:
            wav = None
            mp4 = None
            err = "no video_url in DB"
            if video.video_url:
                print(f"[ig-transcribe] shortcode={video.shortcode} attempt stored URL", flush=True)
                wav, mp4, err = _download_and_extract_audio(video.video_url, tmp)
            if not wav:
                print(f"[ig-transcribe] shortcode={video.shortcode} stored URL failed ({err}) — refetching", flush=True)
                fresh = _fetch_fresh_video_url(video, account)
                if fresh:
                    video.video_url = fresh
                    db.commit()
                    wav, mp4, err = _download_and_extract_audio(fresh, tmp)
                    if not wav:
                        print(f"[ig-transcribe] shortcode={video.shortcode} fresh URL also failed ({err})", flush=True)
                else:
                    err = (err or "") + " (HikerAPI refetch returned no URL)"
            if not wav:
                video.transcription_status = "failed"
                video.transcription_error = (err or "unknown")[:500]
                db.commit()
                return
            # v855 — the reel's own media evidence, taken while the mp4 is here.
            # The matcher CANNOT decide without it, and IG video_urls expire, so
            # "we'll fetch it later" is not a plan.
            fingerprint_downloaded_media(video, mp4, db)
            print(f"[ig-transcribe] shortcode={video.shortcode} starting Whisper", flush=True)
            text = _transcribe_audio(wav)
            print(f"[ig-transcribe] shortcode={video.shortcode} Whisper done ({len(text)}c)", flush=True)
        video.transcription = text
        video.transcription_status = "done"
        video.transcription_error = None
        db.commit()
        # Auto-match: link + advance to published without an operator click ONLY
        # when the margin gate says the top candidate is confident. Ambiguous
        # ones still surface in the suggestions popover for manual confirmation.
        _maybe_auto_match(video, account, db)
        return
    except Exception as exc:
        video.transcription_status = "failed"
        video.transcription_error = str(exc)[:500]
        db.commit()


def process_instagram_transcriptions(db: Session, max_per_tick: int = 1) -> int:
    """Pick up to `max_per_tick` pending InstagramVideo rows and transcribe.
    Returns the number processed."""
    from models import InstagramVideo
    pending = (
        db.query(InstagramVideo)
        .filter(InstagramVideo.transcription_status == "pending")
        .order_by(InstagramVideo.created_at.asc())
        .limit(max_per_tick)
        .all()
    )
    for v in pending:
        transcribe_one(v, db)
    return len(pending)
