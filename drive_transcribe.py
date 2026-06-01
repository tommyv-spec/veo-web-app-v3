# -*- coding: utf-8 -*-
"""Drive folder watcher → transcribe → match → advance to published.

Mirrors instagram_transcribe.py but reads files from a Google Drive folder
instead of HikerAPI. On match >= IG_AUTO_MATCH_THRESHOLD (reused env, default
0.70), advances the matched Job to `published` with `published_via='drive_watch'`.

Critically: does NOT touch instagram_url / instagram_video_id. The IG sync
flow (with widened candidate filter — Slice 3) is responsible for filling
those fields in later when the actual reel posts.
"""
import os
import subprocess
import tempfile
from datetime import datetime
from typing import Optional, Tuple
from sqlalchemy.orm import Session

from drive_client import list_folder_videos, download_file, DriveError

# Reuse instagram_transcribe's Whisper model + matcher path so we don't
# load Whisper twice in the same worker process.
from instagram_transcribe import _model as _whisper_model
_AUTO_MATCH_THRESHOLD = float(os.environ.get("IG_AUTO_MATCH_THRESHOLD", "0.70"))


def _earliest_awaiting_finishing_approval(db: Session, user_id: str) -> Optional[datetime]:
    from models import Job
    row = (
        db.query(Job.approval_at)
        .filter(
            Job.user_id == user_id,
            Job.lifecycle_stage == "awaiting_finishing",
        )
        .order_by(Job.approval_at.asc())
        .first()
    )
    return row[0] if row and row[0] else None


def _extract_audio(mp4_path: str, wav_path: str) -> Tuple[bool, Optional[str]]:
    try:
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", mp4_path, "-vn", "-ac", "1", "-ar", "16000", wav_path],
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


def _maybe_auto_match(video, account, db: Session) -> None:
    """Score against awaiting_finishing jobs. On hit, set published_via='drive_watch'.

    Does NOT touch instagram_url / instagram_video_id — that's the IG matcher's
    job (with widened candidate filter to include drive-watch-published jobs).
    """
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
            )
            .all()
        )
        if not candidates:
            print(f"[drive] file={video.drive_file_id} no awaiting_finishing candidates", flush=True)
            return

        # COALESCE(voiceover_line, dialogue_text) per the v698A fix.
        def _full_dialogue(job):
            rows = (
                db.query(Clip.dialogue_text, Clip.voiceover_line)
                .filter(Clip.job_id == job.id)
                .filter((Clip.clip_role == None) | (Clip.clip_role != 'audio_pair'))  # noqa: E711
                .order_by(Clip.clip_index.asc())
                .all()
            )
            return " ".join(((vo or dt) or "").strip() for dt, vo in rows).strip()

        top = _ig_match.best_matches(
            video, candidates, full_dialogue=_full_dialogue, k=1, min_score=_AUTO_MATCH_THRESHOLD,
        )
        if not top:
            print(f"[drive] file={video.drive_file_id} no candidate >= {_AUTO_MATCH_THRESHOLD}", flush=True)
            return
        match = top[0]
        job = db.query(Job).filter_by(id=match["job_id"]).first()
        if not job:
            return
        video.matched_job_id = job.id
        video.matched_at = datetime.utcnow()
        video.match_score = match.get("score")
        job.lifecycle_stage = "published"
        job.published_via = "drive_watch"
        if job.published_at is None:
            job.published_at = datetime.utcnow()
        db.commit()
        print(f"[drive] AUTO-MATCH file={video.drive_file_id} -> job={job.id[:8]} score={match['score']:.3f}", flush=True)
    except Exception as exc:
        print(f"[drive] auto-match error on file={video.drive_file_id}: {type(exc).__name__}: {str(exc)[:200]}", flush=True)


def transcribe_one(video, db: Session) -> None:
    """Download a DriveVideo, transcribe, auto-match. Idempotent on 'done'."""
    from models import DriveVideo, DriveAccount
    if video.transcription_status == "done":
        return
    video.transcription_status = "running"
    db.commit()
    try:
        account = db.query(DriveAccount).filter_by(id=video.account_id).first()
        if not account:
            video.transcription_status = "failed"
            video.transcription_error = "owning drive account missing"
            db.commit()
            return
        # Posted-before-any-awaiting-finishing-job: nothing to match against.
        cutoff = _earliest_awaiting_finishing_approval(db, account.user_id)
        if cutoff and video.posted_at and video.posted_at < cutoff:
            video.transcription_status = "skipped"
            db.commit()
            return
        from encryption import decrypt as _decrypt
        refresh_token = _decrypt(account.refresh_token_encrypted)
        with tempfile.TemporaryDirectory() as tmp:
            mp4 = os.path.join(tmp, "drive_video.mp4")
            try:
                bytes_written = download_file(refresh_token, video.drive_file_id, mp4)
            except DriveError as e:
                video.transcription_status = "failed"
                video.transcription_error = f"download: {str(e)[:300]}"
                db.commit()
                return
            print(f"[drive] file={video.drive_file_id} downloaded {bytes_written}B", flush=True)
            wav = os.path.join(tmp, "drive_audio.wav")
            ok, err = _extract_audio(mp4, wav)
            if not ok:
                video.transcription_status = "failed"
                video.transcription_error = err[:500]
                db.commit()
                return
            print(f"[drive] file={video.drive_file_id} starting Whisper", flush=True)
            text = _transcribe_wav(wav)
            print(f"[drive] file={video.drive_file_id} Whisper done ({len(text)}c)", flush=True)
        video.transcription = text
        video.transcription_status = "done"
        video.transcription_error = None
        db.commit()
        _maybe_auto_match(video, account, db)
    except Exception as exc:
        video.transcription_status = "failed"
        video.transcription_error = str(exc)[:500]
        db.commit()


def sync_drive_folder(account, db: Session) -> int:
    """List new files in the watched folder, create DriveVideo rows for any
    not yet tracked. Returns the number of new files queued.

    Does NOT transcribe — that's transcribe_one's job, called from the worker
    tick. This keeps each tick step bounded.
    """
    from models import DriveVideo
    from encryption import decrypt as _decrypt
    if not account.folder_id:
        return 0
    refresh_token = _decrypt(account.refresh_token_encrypted)
    # First sync = pull everything. Subsequent syncs only fetch newer files
    # than last_synced_at (Drive's modifiedTime > cutoff).
    files = list_folder_videos(
        refresh_token,
        account.folder_id,
        modified_after=account.last_synced_at,
    )
    queued = 0
    for f in files:
        existing = (
            db.query(DriveVideo)
            .filter_by(account_id=account.id, drive_file_id=f["id"])
            .first()
        )
        if existing:
            continue
        modified_at = None
        if f.get("modifiedTime"):
            try:
                modified_at = datetime.strptime(f["modifiedTime"][:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                modified_at = None
        v = DriveVideo(
            account_id=account.id,
            drive_file_id=f["id"],
            name=f.get("name") or "(unnamed)",
            mime_type=f.get("mimeType"),
            size_bytes=int(f["size"]) if f.get("size") else None,
            posted_at=modified_at,
            transcription_status="pending",
        )
        db.add(v)
        queued += 1
    account.last_synced_at = datetime.utcnow()
    db.commit()
    return queued


def process_drive_transcriptions(db: Session, max_per_tick: int = 1) -> int:
    """Worker tick: sync new files across all DriveAccounts, then transcribe
    up to `max_per_tick` pending DriveVideo rows. Returns transcribed count.
    """
    from models import DriveAccount, DriveVideo
    # 1) Sync every account with a configured folder.
    accounts = (
        db.query(DriveAccount)
        .filter(DriveAccount.folder_id.isnot(None))
        .all()
    )
    for acc in accounts:
        try:
            n = sync_drive_folder(acc, db)
            if n:
                print(f"[drive] account={acc.id} synced — {n} new files queued", flush=True)
        except Exception as exc:
            print(f"[drive] sync error account={acc.id}: {type(exc).__name__}: {str(exc)[:200]}", flush=True)

    # 2) Transcribe up to max_per_tick pending videos.
    pending = (
        db.query(DriveVideo)
        .filter(DriveVideo.transcription_status == "pending")
        .order_by(DriveVideo.created_at.asc())
        .limit(max_per_tick)
        .all()
    )
    for v in pending:
        transcribe_one(v, db)
    return len(pending)
