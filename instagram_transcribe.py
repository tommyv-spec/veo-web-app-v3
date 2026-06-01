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
    """Earliest approval_at across this user's awaiting_finishing Jobs.
    IG videos posted before this time cannot possibly match any candidate."""
    from models import Job
    row = (
        db.query(Job.approval_at)
        .filter(
            Job.user_id == user_id,
            Job.lifecycle_stage == "awaiting_finishing",
            Job.instagram_video_id.is_(None),
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
    /reel/ permalink — that requires auth). Returns (wav_path, error_msg)
    where wav_path is None on failure and error_msg explains why."""
    if not direct_video_url:
        return (None, "no video_url in DB")
    mp4 = os.path.join(work_dir, "ig_video.mp4")
    try:
        with requests.get(direct_video_url, stream=True, timeout=60) as r:
            if r.status_code != 200:
                return (None, f"download HTTP {r.status_code}")
            ctype = r.headers.get("content-type", "")
            with open(mp4, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)
        size = os.path.getsize(mp4) if os.path.exists(mp4) else 0
        print(f"[ig-transcribe] downloaded {size}B content-type={ctype}", flush=True)
    except Exception as e:
        return (None, f"download exception: {type(e).__name__}: {str(e)[:120]}")
    if not os.path.exists(mp4) or os.path.getsize(mp4) < 1024:
        return (None, f"downloaded file too small ({os.path.getsize(mp4) if os.path.exists(mp4) else 0}B)")
    wav = os.path.join(work_dir, "ig_audio.wav")
    try:
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", mp4, "-vn", "-ac", "1", "-ar", "16000", wav],
            check=False, timeout=30,
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            stderr_tail = (result.stderr or "")[-300:]
            return (None, f"ffmpeg rc={result.returncode}: {stderr_tail}")
    except FileNotFoundError:
        return (None, "ffmpeg binary not found on PATH")
    except Exception as e:
        return (None, f"ffmpeg exception: {type(e).__name__}: {str(e)[:120]}")
    return (wav, None)


_AUTO_MATCH_THRESHOLD = float(os.environ.get("IG_AUTO_MATCH_THRESHOLD", "0.70"))


def _maybe_auto_match(video, account, db: Session) -> None:
    """After transcription, score against awaiting_finishing jobs. If the
    top candidate is >= IG_AUTO_MATCH_THRESHOLD (default 0.85), link + advance
    the matched job to published. Lower-confidence matches stay surfaced in
    the manual `/suggestions` UI for operator confirmation."""
    try:
        from models import Job, Clip
        import instagram_match as _ig_match
        if not video.transcription:
            return
        candidates = (
            db.query(Job)
            .filter(
                Job.user_id == account.user_id,
                Job.lifecycle_stage == "awaiting_finishing",
                Job.instagram_video_id.is_(None),
            )
            .all()
        )
        if not candidates:
            print(f"[ig-transcribe] shortcode={video.shortcode} no awaiting_finishing candidates", flush=True)
            return

        def _full_dialogue(job):
            rows = (
                db.query(Clip.dialogue_text)
                .filter(Clip.job_id == job.id)
                .order_by(Clip.clip_index.asc())
                .all()
            )
            return " ".join((r[0] or "") for r in rows).strip()

        top = _ig_match.best_matches(
            video, candidates, full_dialogue=_full_dialogue, k=1, min_score=_AUTO_MATCH_THRESHOLD,
        )
        if not top:
            print(f"[ig-transcribe] shortcode={video.shortcode} no candidate >= {_AUTO_MATCH_THRESHOLD}", flush=True)
            return
        match = top[0]
        job = db.query(Job).filter_by(id=match["job_id"]).first()
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
        print(f"[ig-transcribe] AUTO-MATCH shortcode={video.shortcode} -> job={job.id[:8]} score={match['score']}", flush=True)
    except Exception as exc:
        print(f"[ig-transcribe] auto-match error on shortcode={video.shortcode}: {type(exc).__name__}: {str(exc)[:200]}", flush=True)


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
            err = "no video_url in DB"
            if video.video_url:
                print(f"[ig-transcribe] shortcode={video.shortcode} attempt stored URL", flush=True)
                wav, err = _download_and_extract_audio(video.video_url, tmp)
            if not wav:
                print(f"[ig-transcribe] shortcode={video.shortcode} stored URL failed ({err}) — refetching", flush=True)
                fresh = _fetch_fresh_video_url(video, account)
                if fresh:
                    video.video_url = fresh
                    db.commit()
                    wav, err = _download_and_extract_audio(fresh, tmp)
                    if not wav:
                        print(f"[ig-transcribe] shortcode={video.shortcode} fresh URL also failed ({err})", flush=True)
                else:
                    err = (err or "") + " (HikerAPI refetch returned no URL)"
            if not wav:
                video.transcription_status = "failed"
                video.transcription_error = (err or "unknown")[:500]
                db.commit()
                return
            print(f"[ig-transcribe] shortcode={video.shortcode} starting Whisper", flush=True)
            text = _transcribe_audio(wav)
            print(f"[ig-transcribe] shortcode={video.shortcode} Whisper done ({len(text)}c)", flush=True)
        video.transcription = text
        video.transcription_status = "done"
        video.transcription_error = None
        db.commit()
        # Auto-match: if top candidate >= 0.85, link + advance to published
        # without operator click. Lower-confidence matches still surface in
        # the suggestions popover for manual confirmation.
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
