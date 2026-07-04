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


def _maybe_auto_match(video, db: Session) -> None:
    """Score the transcript against awaiting_finishing jobs. On match, advance."""
    try:
        from models import Job, Clip
        import instagram_match as _ig_match
        if not video.transcription:
            return
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

        # COALESCE(voiceover_line, dialogue_text) per the v698A b-roll fix.
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
            print(f"[local] hash={video.file_hash[:8]} no candidate >= {_AUTO_MATCH_THRESHOLD}", flush=True)
            return
        match = top[0]
        job = db.query(Job).filter_by(id=match["job_id"]).first()
        if not job:
            return
        video.matched_job_id = job.id
        video.matched_at = datetime.utcnow()
        video.match_score = match.get("score")
        job.lifecycle_stage = "published"
        job.published_via = "local_watch"
        if job.published_at is None:
            job.published_at = datetime.utcnow()
        db.commit()
        print(f"[local] AUTO-MATCH hash={video.file_hash[:8]} -> job={job.id[:8]} score={match['score']:.3f}", flush=True)
    except Exception as exc:
        print(f"[local] auto-match error on hash={video.file_hash[:8]}: {type(exc).__name__}: {str(exc)[:200]}", flush=True)


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
