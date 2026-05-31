"""Background transcription pass for pending InstagramVideo rows.

Run inside the existing worker loop (`code/worker.py`). Picks pending
videos one at a time, downloads via yt-dlp, extracts audio via ffmpeg,
transcribes via faster-whisper (int8 CPU), writes the transcript back.
"""
import os
import subprocess
import tempfile
from datetime import datetime
from typing import Optional
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


def _download_and_extract_audio(reel_url: str, work_dir: str) -> Optional[str]:
    """Returns path to a 16kHz mono WAV, or None on failure."""
    mp4 = os.path.join(work_dir, "ig_video.mp4")
    try:
        subprocess.run(
            ["yt-dlp", "-q", "-f", "mp4/best", "-o", mp4, reel_url],
            check=True, timeout=60,
        )
    except Exception:
        return None
    wav = os.path.join(work_dir, "ig_audio.wav")
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", mp4, "-vn", "-ac", "1", "-ar", "16000", wav],
            check=True, timeout=30,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception:
        return None
    return wav


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
            wav = _download_and_extract_audio(video.url, tmp)
            if not wav:
                video.transcription_status = "failed"
                video.transcription_error = "download or audio-extract failed"
                db.commit()
                return
            text = _transcribe_audio(wav)
        video.transcription = text
        video.transcription_status = "done"
        video.transcription_error = None
        db.commit()
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
