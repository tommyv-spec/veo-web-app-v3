# -*- coding: utf-8 -*-
"""
Veo Web App - Main FastAPI Application

Features:
- REST API for job management
- Server-Sent Events for real-time progress
- File upload handling
- Static file serving
- Password protection for private access
"""

import os
import sys
import json
import uuid
import shutil
import secrets
import hashlib
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
from contextlib import asynccontextmanager


# =============================================================================
# Helper Functions
# =============================================================================
def safe_images_dir(images_dir: Union[str, None]) -> Union[Path, None]:
    """
    Safely convert images_dir to Path, returning None for empty/blank strings.
    
    CRITICAL: Never call Path() on empty strings!
    Path("") becomes Path(".") which searches the current directory,
    leading to errors for Flow jobs where frames are in R2, not local disk.
    """
    if not images_dir or not images_dir.strip():
        return None
    return Path(images_dir)


# =============================================================================
# FFmpeg Setup - Cross-platform (Windows + Linux)
# =============================================================================
def setup_ffmpeg():
    """Set up FFMPEG_BIN and FFPROBE_BIN environment variables."""
    # Check if already set and valid
    if os.environ.get("FFMPEG_BIN"):
        ffmpeg_path = os.environ["FFMPEG_BIN"]
        if Path(ffmpeg_path).exists() or shutil.which(ffmpeg_path):
            print(f"[FFmpeg] Using configured: {ffmpeg_path}")
            return
    
    # Check if ffmpeg is in PATH (Linux/Docker typically)
    ffmpeg_in_path = shutil.which("ffmpeg")
    ffprobe_in_path = shutil.which("ffprobe")
    
    if ffmpeg_in_path:
        os.environ["FFMPEG_BIN"] = ffmpeg_in_path
        if ffprobe_in_path:
            os.environ["FFPROBE_BIN"] = ffprobe_in_path
        print(f"[FFmpeg] Found in PATH: {ffmpeg_in_path}")
        return
    
    # Windows-specific search
    if sys.platform == "win32":
        possible_paths = []
        
        # Check ImageIO_FFMPEG_EXE first (might be set by user)
        if os.environ.get("ImageIO_FFMPEG_EXE"):
            possible_paths.append(os.environ["ImageIO_FFMPEG_EXE"])
        
        # Common Windows installation paths
        possible_paths.extend([
            r"C:\ffmpeg\bin\ffmpeg.exe",
            r"C:\ffmpeg\ffmpeg.exe",
            r"C:\Program Files\ffmpeg\bin\ffmpeg.exe",
            r"C:\Program Files (x86)\ffmpeg\bin\ffmpeg.exe",
        ])
        
        # Search in C:\ffmpeg for any ffmpeg.exe
        ffmpeg_base = Path(r"C:\ffmpeg")
        if ffmpeg_base.exists():
            for found in ffmpeg_base.glob("**/ffmpeg.exe"):
                possible_paths.append(str(found))
        
        for ffmpeg_path in possible_paths:
            if ffmpeg_path and Path(ffmpeg_path).exists():
                ffmpeg_path = str(ffmpeg_path)
                ffprobe_path = str(Path(ffmpeg_path).parent / "ffprobe.exe")
                
                os.environ["FFMPEG_BIN"] = ffmpeg_path
                if Path(ffprobe_path).exists():
                    os.environ["FFPROBE_BIN"] = ffprobe_path
                
                # Also add to PATH
                bin_dir = str(Path(ffmpeg_path).parent)
                if bin_dir not in os.environ.get("PATH", ""):
                    os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
                
                print(f"[FFmpeg] Found: {ffmpeg_path}")
                return
    
    # Linux - ffmpeg should be installed via apt
    print("[FFmpeg] Warning: ffmpeg not found. Install with: apt-get install ffmpeg")

# Run ffmpeg setup
setup_ffmpeg()

# =============================================================================
# Authentication Configuration (Google OAuth)
# =============================================================================
# Set these environment variables for Google OAuth:
# GOOGLE_CLIENT_ID - Google OAuth client ID
# GOOGLE_CLIENT_SECRET - Google OAuth client secret
# SESSION_SECRET - Secret key for sessions (auto-generated if not set)
# APP_URL - Your app URL (e.g., https://your-app.onrender.com)

from auth import (
    GOOGLE_AUTH_ENABLED, oauth, SESSION_SECRET,
    get_current_user, get_optional_user, validate_session,
    handle_google_login, handle_google_callback, delete_session
)

# =============================================================================
# FastAPI Imports and Setup
# =============================================================================
from fastapi import (
    FastAPI, HTTPException, UploadFile, File, Form, 
    BackgroundTasks, Depends, Query, Request, Response, Cookie, Header
)
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session as DBSession

from config import (
    app_config, VideoConfig, APIKeysConfig, DialogueLine,
    JobStatus, ClipStatus, SUPPORTED_IMAGE_FORMATS,
    MAX_IMAGE_SIZE_BYTES, AspectRatio, Resolution, Duration,
    ApprovalStatus, api_keys_config
)
from models import (
    init_db, get_db_session, Job, Clip, JobLog, BlacklistEntry,
    get_job_logs_since, add_job_log, User, UserAPIKey, UserWorkerToken
)
from worker import worker, WORKER_VERSION
from error_handler import ErrorCode

# Image Platform (node-graph image generation via Flow UI worker)
from image_platform import (
    router as image_router,
    watch_done_files_loop as _image_watch_done_files_loop,
    run_image_platform_migrations as _run_image_platform_migrations,
    cleanup_orphan_nodes as _cleanup_image_orphans,
)


# ============ Pydantic Models ============

class DialogueLineInput(BaseModel):
    id: int
    text: str
    start_image_idx: Optional[int] = None  # Storyboard image assignment
    scene_index: Optional[int] = None      # Which scene this clip belongs to
    clip_mode: Optional[str] = "blend"     # 'blend' | 'continue' | 'fresh'
    scene_transition: Optional[str] = None # 'blend' | 'cut' | null (for first scene)
    action_note: Optional[str] = None      # Custom director action (overrides auto gesture/transition)
    # v572 — per-clip Veo prompt overrides. When `veo_prompt_override`
    # is non-empty, build_prompt's auto-construction is BYPASSED and
    # this string is shipped to Veo verbatim. The optional
    # `veo_negative_prompt_override`, if non-empty, is concatenated
    # as a "Negative prompt:" trailer to fit the existing single-string
    # Veo API path. Both fields are typically populated automatically
    # when promoting an image batch from a markdown that has a
    # `## Veo 3.1 Final Prompts (per clip)` section, but they can also
    # be set per-line via the API for ad-hoc overrides on individual
    # clips. Leaving them None preserves pre-v572 behavior.
    veo_prompt_override: Optional[str] = None
    veo_negative_prompt_override: Optional[str] = None
    # v644 — per-line audio-padding suffix. When set, the Veo prompt
    # builder appends `" " + dialogue_pad` after the line so Veo's
    # experimental audio path has enough text to reliably synthesize
    # speech (it tends to fail on short lines, especially on Fast Lower
    # Priority tier). Whisper-VAD continues to use the bare `text` as
    # script truth, so the pad's spoken audio is automatically trimmed
    # from the final cut by the existing apply_vad pipeline as
    # unmatched filler. Optional; if None, Veo prompt uses bare line.
    dialogue_pad: Optional[str] = None
    # v667/v668 — transformation-video metadata. cut_mode is the per-clip
    # trim strategy ('whisper' | 'timeline' | 'auto' | null → whisper).
    # target_duration_s is the anchor-derived target duration computed by
    # prepare_batch_for_video from consecutive ImageNode.frame_anchor_s
    # values. veo_render_duration_s is ceil_to(target_duration_s, [4,6,8])
    # — the Veo render-bucket pick. All optional; null = legacy whisper-VAD
    # path with no anchor-driven duration override.
    cut_mode: Optional[str] = None
    target_duration_s: Optional[float] = None
    veo_render_duration_s: Optional[int] = None
    # v681 — text-card / caption denorm. caption is informational on
    # shot clips and rendered text on text_card clips. scene_type
    # 'text_card' bypasses Veo render and triggers the ffmpeg
    # drawtext path. bg_color is the solid-color background for
    # text_card clips (CSS color or hex).
    caption: Optional[str] = None
    scene_type: Optional[str] = None
    bg_color: Optional[str] = None
    # v681 — text_card duration in seconds. When scene_type='text_card'
    # this overrides the renderer's 1.0s default. Stored on Clip rows
    # via target_duration_s (overloaded for text_card; legacy clips
    # use target_duration_s for v667/v668 anchor-derived Veo trim).
    duration_s: Optional[float] = None


class SceneInput(BaseModel):
    sceneIndex: int
    # v682e — imageIndex is Optional because text_card scenes have no
    # uploaded image (they render via ffmpeg drawtext at video assembly,
    # not Veo). Pre-v682e the Pydantic int requirement rejected the
    # whole job-creation request when ANY text_card scene was in the
    # storyboard, with the error:
    #   `body.scenes[N].imageIndex: Input should be a valid integer, input:null`
    imageIndex: Optional[int] = None
    clipMode: str = "blend"        # 'blend' | 'continue' | 'fresh'
    transition: Optional[str] = None  # 'blend' | 'cut' | null for first scene
    clips: List[int] = []          # List of clip indices in this scene
    # v682e — scene_type denorm so the backend can branch on text_card
    # without inferring from imageIndex==None alone (more explicit and
    # less error-prone). Mirrors DialogueLineInput.scene_type.
    scene_type: Optional[str] = None


class VideoConfigInput(BaseModel):
    aspect_ratio: str = "9:16"
    resolution: str = "720p"
    duration: str = "8"
    language: str = "English"
    use_interpolation: bool = True
    use_openai_prompt_tuning: bool = True
    use_frame_vision: bool = True
    max_retries_per_clip: int = 5
    custom_prompt: str = ""  # User's custom prompt when AI is disabled
    user_context: str = ""  # User context for AI prompt generation
    single_image_mode: bool = False  # Use same image for start/end frames
    storyboard_mode: bool = False    # Whether in storyboard mode
    generation_mode: str = "parallel"  # "parallel" (fast) or "sequential" (guaranteed smooth transitions)
    backend_preference: str = "auto"  # "auto", "api", or "flow"
    flow_variants_count: int = 2  # How many variants per clip in Flow (x1/x2/x3/x4)
    use_gesture_enrichment: bool = False  # Generate content-specific gesture cues via LLM
    short_dialogue_mode: str = "optimized"  # "optimized" = timed speech + silence, "fill" = pad to 25 words
    # v539 — Prefix Short Lines
    # Prepend a leading word (default "only") to dialogue lines whose word
    # count is strictly below `prefix_short_threshold`. Used to give Veo a
    # clean consonant onset and to mitigate the first-word-truncation
    # symptom seen at clip boundaries.
    prefix_short_enabled: bool = False
    prefix_short_word: str = "only"
    prefix_short_threshold: int = 15


class APIKeysInput(BaseModel):
    gemini_keys: List[str] = []
    openai_key: Optional[str] = None


class CreateJobRequest(BaseModel):
    config: VideoConfigInput
    dialogue_lines: List[DialogueLineInput]
    api_keys: APIKeysInput
    job_id: Optional[str] = None  # Use existing upload job_id if provided
    scenes: Optional[List[SceneInput]] = None  # Scene definitions for storyboard mode
    last_frame_index: Optional[int] = None  # Index of image to use as end frame for the video
    # v475: if this job originates from an image batch ("Prepare for video"
    # flow), the frontend passes the batch_id so we can stamp
    # ImageJobBatch.promoted_video_job_id and the sidebar's 🎥 badge can
    # link the two. Before this, there was no link between the batch and
    # the video job it generated — the prepare flow didn't use the
    # /batches/{id}/promote-to-video path which was the only place that
    # stamped the link.
    image_batch_id: Optional[str] = None


class JobResponse(BaseModel):
    id: str
    status: str
    progress_percent: float
    total_clips: int
    completed_clips: int
    failed_clips: int
    skipped_clips: int
    created_at: Optional[str]
    started_at: Optional[str]
    completed_at: Optional[str]
    backend: Optional[str] = None
    first_dialogue: Optional[str] = None
    first_frame_url: Optional[str] = None
    has_export: bool = False
    has_voice_clone: bool = False


class ClipResponse(BaseModel):
    id: int
    clip_index: int
    dialogue_id: int
    dialogue_text: str
    status: str
    retry_count: int
    start_frame: Optional[str]
    end_frame: Optional[str]
    output_filename: Optional[str]
    error_code: Optional[str]
    error_message: Optional[str]
    # New approval fields
    approval_status: str = "pending_review"
    generation_attempt: int = 1
    attempts_remaining: int = 2
    redo_reason: Optional[str] = None
    versions: List[Dict] = []
    # Variant fields
    selected_variant: int = 1
    total_variants: int = 0
    # Scene/mode fields
    clip_mode: Optional[str] = "blend"
    scene_index: Optional[int] = 0
    # Prompt
    prompt_text: Optional[str] = None
    # Lineup
    in_lineup: bool = True


class RedoRequest(BaseModel):
    reason: Optional[str] = None  # Optional reason for redo
    new_dialogue: Optional[str] = None  # Optional new dialogue text for the clip


class ApprovalResponse(BaseModel):
    clip_id: int
    status: str
    message: str
    attempts_remaining: int


class LogResponse(BaseModel):
    id: int
    created_at: str
    level: str
    category: Optional[str]
    clip_index: Optional[int]
    message: str


class ErrorResponse(BaseModel):
    code: str
    message: str
    details: Optional[Dict] = None


# ============ Application Setup ============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management.

    v473: startup is split into two phases.
    PHASE 1 (blocks serving): the minimum needed for FastAPI to respond
    correctly — DB init, schema migrations. Anything that could break
    endpoints if not run must go here.
    PHASE 2 (runs in background after yield): everything else — orphan
    cleanup, user-id backfills, stuck-job recovery, flow-worker file
    sync, etc. These all can tolerate running a few seconds late, and
    moving them out of the critical path means Render's health check
    passes in seconds instead of tens of seconds. Deploy outage window
    shrinks accordingly.
    """
    # ========== PHASE 1: BLOCKING (required before serving) ==========
    init_db()

    # Image platform schema migrations (adds claim columns on image_nodes
    # for existing DBs that were created with older versions).
    # MUST block: endpoints that query ImageNode need these columns.
    try:
        _run_image_platform_migrations()
    except Exception as _ipme:
        print(f"[Migration] image_platform: {_ipme}", flush=True)

    # Auto-migrate: add clip_order_json if missing
    # MUST block: job endpoints access this column.
    try:
        from migrations.add_clip_order import migrate as _migrate_clip_order
        _migrate_clip_order()
    except Exception as _mig_e:
        print(f"[Migration] clip_order_json: {_mig_e}", flush=True)

    # === VERSION INFO - Proves which code is deployed ===
    print(f"[Build] WORKER_VERSION={WORKER_VERSION}", flush=True)
    print(f"[Build] RENDER_GIT_COMMIT={os.environ.get('RENDER_GIT_COMMIT', 'not set')}", flush=True)
    print(f"[Build] IMAGE_TAG={os.environ.get('IMAGE_TAG', 'not set')}", flush=True)

    worker.start()
    print("[App] Started — serving HTTP. Deferred startup tasks queued.")

    # ========== PHASE 2: DEFERRED (runs in background) ==========
    # Everything below yields control back to the event loop before each
    # chunk of work so HTTP requests get interleaved. FastAPI is already
    # healthy from Render's perspective by the time this runs.
    import asyncio as _asyncio

    async def _run_deferred_startup():
        """Non-blocking startup housekeeping. Yields between steps so
        serving requests get priority."""
        # Small initial delay — let the event loop drain any startup
        # handshake / health-check requests before we load it with DB work.
        await _asyncio.sleep(0.5)

        # Image platform: clean up orphan variants/nodes whose files were
        # wiped by an ephemeral filesystem (Render redeploys). As of v466
        # this is HEAD-only and fast per variant, but with hundreds of
        # variants it still adds up. Running in background means startup
        # doesn't wait on R2 HEAD latency.
        try:
            _cleanup_result = await _asyncio.to_thread(_cleanup_image_orphans)
            if _cleanup_result and (_cleanup_result.get("removed_variants") or _cleanup_result.get("removed_nodes")):
                print(f"[Deferred] Cleanup: removed {_cleanup_result['removed_variants']} orphan variants, {_cleanup_result['removed_nodes']} orphan nodes", flush=True)
        except Exception as _ipce:
            print(f"[Deferred] image_platform cleanup: {_ipce}", flush=True)

        # Backfill user_id on legacy video jobs. Safe to run late —
        # only affects ownership checks on legacy rows. Jobs created
        # post-v447 already have user_id set.
        try:
            from models import User as _User, Job as _Job, get_db as _get_db
            def _do_backfill():
                with _get_db() as _db:
                    _first = _db.query(_User).filter(_User.is_active == True).order_by(_User.created_at.asc()).first()  # noqa: E712
                    if _first is not None:
                        _n = _db.query(_Job).filter(_Job.user_id.is_(None)).count()
                        if _n:
                            _db.query(_Job).filter(_Job.user_id.is_(None)).update(
                                {"user_id": _first.id}, synchronize_session=False
                            )
                            _db.commit()
                            return (_n, _first.id, _first.email or _first.name or "?")
                return None
            _res = await _asyncio.to_thread(_do_backfill)
            if _res:
                print(f"[Deferred] Backfilled {_res[0]} job(s) with user_id={_res[1]} ({_res[2]})", flush=True)
        except Exception as _jbf_e:
            print(f"[Deferred] jobs user_id backfill: {_jbf_e}", flush=True)

        # v487: rescue orphaned redos. Any Job that has a clip in
        # flow_redo_queued but whose own updated_at is older than 24h
        # can't be picked up by the worker (the 24h activity filter).
        # Touch those jobs' updated_at to NOW so their pending redos
        # become visible on the next poll. One-shot housekeeping —
        # doesn't re-run, doesn't hurt anything if there's nothing to
        # rescue. Without this, pre-v485 redos stay stuck forever even
        # after the redo endpoint starts bumping updated_at for new
        # redos.
        try:
            from models import Clip as _Clip, Job as _Job2, get_db as _get_db2
            from datetime import datetime as _dt, timedelta as _td
            def _rescue_orphaned_redos():
                with _get_db2() as _db:
                    _stale_cutoff = _dt.utcnow() - _td(hours=24)
                    # Find distinct job_ids that have flow_redo_queued clips
                    # AND whose job.updated_at is older than the cutoff.
                    stale_job_ids = [row[0] for row in _db.query(_Clip.job_id).join(_Job2).filter(
                        _Clip.status == 'flow_redo_queued',
                        _Job2.updated_at < _stale_cutoff,
                    ).distinct().all()]
                    if stale_job_ids:
                        _db.query(_Job2).filter(_Job2.id.in_(stale_job_ids)).update(
                            {"updated_at": _dt.utcnow()}, synchronize_session=False
                        )
                        _db.commit()
                    return len(stale_job_ids)
            _rescued = await _asyncio.to_thread(_rescue_orphaned_redos)
            if _rescued:
                print(f"[Deferred] Rescued {_rescued} job(s) with orphaned redos — bumped updated_at so workers can see them", flush=True)
        except Exception as _ror_e:
            print(f"[Deferred] orphan-redo rescue: {_ror_e}", flush=True)

        # Sync flow worker files (local_flow_worker.py → static/flow_worker.py).
        # This must finish before any worker download from the /static path,
        # but the server starts serving HTTP before workers actually hit
        # that endpoint, so moving it here is fine in practice.
        try:
            def _do_sync():
                local_worker = Path(__file__).parent / "local_flow_worker.py"
                static_worker = Path(__file__).parent / "static" / "flow_worker.py"
                if local_worker.exists():
                    import shutil, hashlib
                    shutil.copy2(str(local_worker), str(static_worker))
                    build_hash = hashlib.md5(local_worker.read_bytes()).hexdigest()[:12]
                    return build_hash
                return None
            _bh = await _asyncio.to_thread(_do_sync)
            if _bh:
                print(f"[Deferred] Flow worker synced: local_flow_worker.py → static/flow_worker.py ({_bh})", flush=True)
        except Exception as _fs_e:
            print(f"[Deferred] flow worker sync: {_fs_e}", flush=True)

        # Recover jobs stuck in "preparing" (killed by deploy/restart).
        try:
            from models import get_db
            def _recover_stuck():
                with get_db() as _rdb:
                    _five_min_ago = datetime.utcnow() - timedelta(minutes=5)
                    _stuck_jobs = _rdb.query(Job).filter(
                        Job.status == 'preparing',
                        Job.started_at < _five_min_ago
                    ).all()
                    if _stuck_jobs:
                        for _sj in _stuck_jobs:
                            _sj.status = JobStatus.FAILED.value
                            _sj.error_message = "Server restarted during preparation — please retry by cloning this job."
                        _rdb.commit()
                        return len(_stuck_jobs)
                return 0
            _n_stuck = await _asyncio.to_thread(_recover_stuck)
            if _n_stuck:
                print(f"[Deferred] Recovered {_n_stuck} stuck job(s) from 'preparing' state", flush=True)
        except Exception as _re:
            print(f"[Deferred] stuck-job recovery: {_re}", flush=True)

        # v564 NOTE: link rebuild deliberately NOT auto-run on startup.
        # An earlier draft of this file (briefly considered) included a
        # one-shot rebuild that would call _backfill_batch_video_links_impl
        # for every user on deploy, to restore the batch→video-job links
        # v563 deleted. That was tempting but wrong — v562's proximity
        # algorithm IS the bug that mis-linked the user's prostate batch
        # in the first place (matched a never-promoted batch to an
        # unrelated video job within 6h). Re-running it on deploy would
        # re-introduce the same wrong links. There is no clean ground
        # truth in the current data model (verified: 0 of 50 jobs have
        # imported_from_batch in config_json), so any rebuild is a
        # guess. Manual relinking via /api/images/batches/{id}/link-video
        # is the safe path until a content-based linker (using
        # Clip.start_frame back-trace to ImageVariant→ImageNode→batch)
        # is built and tested.
        print("[Deferred] ✓ All deferred startup tasks complete", flush=True)

    # Fire the deferred work without awaiting it — it runs concurrently
    # with HTTP request handling.
    _asyncio.create_task(_run_deferred_startup())

    # Background task: purge logs for completed jobs older than 24h
    # job_logs grows unbounded otherwise and causes OOM on 512MB instances
    import asyncio as _asyncio
    async def _purge_old_logs():
        while True:
            await _asyncio.sleep(3600)  # run every hour
            try:
                from models import get_db, JobLog, Job
                from sqlalchemy import delete
                cutoff = datetime.utcnow() - timedelta(hours=24)
                with get_db() as _db:
                    old_job_ids = [
                        j.id for j in _db.query(Job.id).filter(
                            Job.status.in_(['completed', 'failed', 'cancelled']),
                            Job.updated_at < cutoff
                        ).all()
                    ]
                    if old_job_ids:
                        deleted = _db.query(JobLog).filter(
                            JobLog.job_id.in_(old_job_ids)
                        ).delete(synchronize_session=False)
                        _db.commit()
                        if deleted:
                            print(f"[Purge] Deleted {deleted} old job logs for {len(old_job_ids)} completed jobs", flush=True)
            except Exception as _pe:
                print(f"[Purge] Log purge error: {_pe}", flush=True)

    _purge_task = _asyncio.create_task(_purge_old_logs())

    # Image platform: background poller for .done.json files from image_worker.py
    _image_stop_event = _asyncio.Event()
    _image_watch_task = _asyncio.create_task(_image_watch_done_files_loop(_image_stop_event))
    print("[App] Image platform watch task started", flush=True)

    yield
    
    # Shutdown
    _purge_task.cancel()
    _image_stop_event.set()
    try:
        await _asyncio.wait_for(_image_watch_task, timeout=3.0)
    except Exception:
        _image_watch_task.cancel()
    worker.stop()
    print("[App] Shutdown complete")


app = FastAPI(
    title="KavenoBuilder",
    description="KavenoBuilder — AI video generation platform",
    version="1.31.10",
    lifespan=lifespan,
)

# Mount the image platform router (/api/images/...)
app.include_router(image_router)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Request Logging Middleware
# =============================================================================
import re as _re
from starlette.middleware.base import BaseHTTPMiddleware

class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log requests selectively — suppress polling and asset-serving noise."""

    # Paths to never log (hit constantly, zero information value)
    _SILENT_PATHS = {
        "/api/health",
        "/favicon.ico",
        "/static/favicon.png",
        # Image worker heartbeat — POST, so not caught by GET-based
        # poll-pattern silencer below. Fires every ~30s per worker.
        "/api/images/worker/heartbeat",
    }

    # Patterns for routine polling (suppress unless error)
    _POLL_PATTERNS = _re.compile(
        r"/api/jobs$"                           # Job list
        r"|/api/jobs/[^/]+$"                    # Single job status
        r"|/api/jobs/[^/]+/clips$"              # Clips list
        r"|/api/jobs/[^/]+/logs"                # Logs poll
        r"|/api/jobs/[^/]+/review-status$"      # Review status
        r"|/api/jobs/[^/]+/images/"             # Image serving
        r"|/api/jobs/[^/]+/outputs/"            # Video serving (206)
        r"|/api/user-worker/jobs/pending"       # Worker job poll
        r"|/api/user-worker/clips/redo-pending" # Worker redo poll
        r"|/api/local-worker/jobs/pending"      # Local worker job poll
        r"|/api/local-worker/clips/redo-pending"# Local worker redo poll
        r"|/api/voice-clone-warmup"             # Warmup ping
        # Image platform polling — frontend + worker
        r"|/api/images/nodes$"                  # Sidebar node list poll
        r"|/api/images/nodes/[0-9]+$"           # Single node detail poll
        r"|/api/images/graph$"                  # Graph view poll
        r"|/api/images/batches$"                # Batch list poll
        r"|/api/images/batches/[^/]+$"          # Single batch detail
        r"|/api/images/worker/status"           # Status-strip poll (frontend)
        r"|/api/images/worker/jobs/pending"     # Worker job poll
        r"|/api/images/files/"                  # Variant image serving
    )

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Total silence
        if path in self._SILENT_PATHS:
            return await call_next(request)

        method = request.method
        response = await call_next(request)
        status = response.status_code

        # Suppress routine GET polling unless it errored
        if method == "GET" and status < 400 and self._POLL_PATTERNS.search(path):
            return response

        # Log everything else (POST, errors, exports, uploads, etc.)
        client = request.client.host if request.client else "?"
        worker_id = request.query_params.get("worker_id", "")
        job_match = _re.search(
            r"/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
            path, _re.I
        )
        job_id = job_match.group(1)[:8] + "..." if job_match else ""
        ctx_parts = []
        if worker_id:
            ctx_parts.append(f"worker={worker_id}")
        if job_id:
            ctx_parts.append(f"job={job_id}")
        ctx = f" [{', '.join(ctx_parts)}]" if ctx_parts else ""
        print(f"[HTTP] {method} {path} → {status} | {client}{ctx}", flush=True)

        return response

app.add_middleware(RequestLoggingMiddleware)

# Add session middleware for OAuth (required by authlib)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)

# Redirect old Render URL to kavenobuilder.com (browser requests only)
class DomainRedirectMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        host = request.headers.get("host", "")
        if "onrender.com" in host:
            path = request.url.path
            # Never redirect API/worker calls — their HTTP clients drop auth headers on 301
            if not path.startswith("/api/") and not path.startswith("/auth/"):
                new_url = str(request.url).replace(host, "kavenobuilder.com")
                from starlette.responses import RedirectResponse as _RR
                return _RR(new_url, status_code=301)
        return await call_next(request)

app.add_middleware(DomainRedirectMiddleware)

class AuthMiddleware(BaseHTTPMiddleware):
    """Middleware to protect routes with Google OAuth authentication."""
    
    # Routes that don't require authentication
    PUBLIC_ROUTES = {
        "/login", "/auth/login", "/auth/google/callback", 
        "/auth/me", "/api/health", "/favicon.ico"
    }
    PUBLIC_PREFIXES = {"/static/", "/auth/", "/api/local-worker/", "/api/user-worker/", "/api/images/worker/"}
    
    async def dispatch(self, request: Request, call_next):
        # Skip auth if Google OAuth is not configured
        if not GOOGLE_AUTH_ENABLED:
            return await call_next(request)
        
        path = request.url.path
        
        # Allow public routes
        if path in self.PUBLIC_ROUTES:
            return await call_next(request)
        
        # Allow routes with public prefixes
        for prefix in self.PUBLIC_PREFIXES:
            if path.startswith(prefix):
                return await call_next(request)
        
        # Check session cookie
        session_token = request.cookies.get("session")
        
        # (Debug cookie-check log removed — was firing on every root page hit)
        
        if session_token:
            # Check in-memory cache first to avoid DB connection per request
            import time as _time
            if not hasattr(AuthMiddleware, '_session_cache'):
                AuthMiddleware._session_cache = {}
            
            cache_key = session_token
            cached = AuthMiddleware._session_cache.get(cache_key)
            if cached and (_time.time() - cached['ts']) < 60:  # Cache valid for 60s
                if cached['valid']:
                    return await call_next(request)
            
            # Cache miss or expired — validate against DB
            from models import get_db, User, UserSession
            from auth import validate_session as db_validate_session
            with get_db() as db:
                user = db_validate_session(db, session_token)
                if user and user.is_active:
                    AuthMiddleware._session_cache[cache_key] = {'valid': True, 'ts': _time.time()}
                    return await call_next(request)
                else:
                    AuthMiddleware._session_cache[cache_key] = {'valid': False, 'ts': _time.time()}
                    print(f"[AuthMiddleware] Session invalid or user inactive for token: {session_token[:8]}...", flush=True)
            
            # Evict expired entries if cache is getting large (cap at 500)
            if len(AuthMiddleware._session_cache) > 500:
                now = _time.time()
                AuthMiddleware._session_cache = {
                    k: v for k, v in AuthMiddleware._session_cache.items()
                    if now - v['ts'] < 60
                }
        
        # Not authenticated - redirect to login or return 401
        if path.startswith("/api/"):
            return Response(
                content=json.dumps({"detail": "Not authenticated"}),
                status_code=401,
                media_type="application/json"
            )
        else:
            return RedirectResponse(url="/login", status_code=302)

# Add auth middleware (only if Google OAuth is configured)
if GOOGLE_AUTH_ENABLED:
    app.add_middleware(AuthMiddleware)


# ============ Static Files ============

# Create static directory if not exists
static_dir = app_config.base_dir / "static"
static_dir.mkdir(exist_ok=True)

# Mount static files
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# ============ Version Endpoint (for deployment verification) ============

@app.get("/api/version")
def get_version():
    """Return version info to verify which code is deployed"""
    return {
        "app": "veo-web-app",
        "worker_version": WORKER_VERSION,
        "render_commit": os.environ.get("RENDER_GIT_COMMIT", "not set"),
    }


@app.post("/api/admin/cleanup-stale-redos")
async def cleanup_stale_redos(
    hours: int = Query(0, description="Only clean redos older than N hours. 0 = all."),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Clean up flow_redo_queued clips. hours=0 cleans all, hours=24 cleans >24h old."""
    query = db.query(Clip).join(Job).filter(
        Job.user_id == current_user.id,
        Clip.status == 'flow_redo_queued',
    )
    if hours > 0:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        query = query.filter(Job.updated_at < cutoff)
    stale = query.all()
    count = len(stale)
    for clip in stale:
        clip.status = 'failed'
        clip.error_message = 'Stale redo cleaned up'
    db.commit()
    return {"cleaned": count}


@app.post("/api/admin/fix-orphaned-clips")
async def fix_orphaned_clips(
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Fix completed clips that have versions uploaded but no output_filename set.
    This happens when variant 1 fails to download but variant 2 succeeds."""
    broken = db.query(Clip).join(Job).filter(
        Job.user_id == current_user.id,
        Clip.status.in_(['completed', 'approved']),
        Clip.output_filename.is_(None),
        Clip.versions_json.isnot(None),
    ).all()
    fixed = 0
    for clip in broken:
        try:
            versions = json.loads(clip.versions_json) if clip.versions_json else []
            if versions:
                # Pick first available version
                v = versions[0]
                clip.output_filename = v.get('filename')
                clip.output_url = v.get('url')
                clip.selected_variant = 1
                fixed += 1
        except Exception:
            pass
    db.commit()
    return {"fixed": fixed, "checked": len(broken)}


@app.post("/api/admin/backfill-badges")
async def backfill_export_voice_badges(
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Backfill has_export/has_voice_clone flags by scanning R2 outputs for each job."""
    from backends.storage import is_storage_configured, get_storage
    
    if not is_storage_configured():
        return {"error": "Storage not configured"}
    
    storage = get_storage()
    jobs = db.query(Job).filter(
        Job.user_id == current_user.id,
        Job.status.in_(['completed', 'processing']),
    ).all()
    
    export_count = 0
    voice_count = 0
    for job in jobs:
        if getattr(job, 'has_export', False) and getattr(job, 'has_voice_clone', False):
            continue  # Already flagged
        try:
            r2_prefix = f"jobs/{job.id}/outputs/"
            keys = []
            if hasattr(storage, 'list_objects'):
                keys = storage.list_objects(r2_prefix)
            elif hasattr(storage, 'client'):
                resp = storage.client.list_objects_v2(Bucket=storage.bucket_name, Prefix=r2_prefix, MaxKeys=200)
                keys = [obj["Key"] for obj in resp.get("Contents", [])]
            
            filenames = [k.replace(r2_prefix, "") for k in keys if k.replace(r2_prefix, "")]
            
            if not getattr(job, 'has_export', False):
                has_exp = any(f.startswith("final_") or f.startswith("export_") for f in filenames)
                if has_exp:
                    job.has_export = True
                    export_count += 1
            
            if not getattr(job, 'has_voice_clone', False):
                has_vc = any("voice_cloned" in f or "voice_swapped" in f for f in filenames)
                if has_vc:
                    job.has_voice_clone = True
                    voice_count += 1
        except Exception:
            pass
    
    db.commit()
    return {"jobs_scanned": len(jobs), "exports_found": export_count, "voice_clones_found": voice_count}


@app.get("/login", response_class=HTMLResponse)
async def login_page():
    """Serve the login page with Google Sign-In"""
    return HTMLResponse("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login - KavenoBuilder</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f0f23 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            color: #fff;
        }
        .login-container {
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 16px;
            padding: 40px;
            width: 100%;
            max-width: 400px;
            backdrop-filter: blur(10px);
            text-align: center;
        }
        .logo {
            margin-bottom: 30px;
        }
        .logo h1 {
            font-size: 32px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 8px;
        }
        .logo .subtitle {
            color: rgba(255,255,255,0.6);
            font-size: 14px;
        }
        .divider {
            display: flex;
            align-items: center;
            margin: 30px 0;
            color: rgba(255,255,255,0.4);
            font-size: 13px;
        }
        .divider::before, .divider::after {
            content: '';
            flex: 1;
            height: 1px;
            background: rgba(255,255,255,0.1);
        }
        .divider span { padding: 0 15px; }
        
        .google-btn {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 12px;
            width: 100%;
            padding: 14px 20px;
            background: #fff;
            border: none;
            border-radius: 8px;
            color: #333;
            font-size: 16px;
            font-weight: 500;
            cursor: pointer;
            text-decoration: none;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        .google-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 20px rgba(255,255,255,0.2);
        }
        .google-btn:active {
            transform: translateY(0);
        }
        .google-btn svg {
            width: 20px;
            height: 20px;
        }
        .info {
            margin-top: 24px;
            font-size: 13px;
            color: rgba(255,255,255,0.4);
        }
        .error {
            background: rgba(255,59,48,0.2);
            border: 1px solid rgba(255,59,48,0.3);
            color: #ff6b6b;
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 20px;
            font-size: 14px;
            display: none;
        }
        .error.show { display: block; }
    </style>
</head>
<body>
    <div class="login-container">
        <div class="logo">
            <h1>🎬 KavenoBuilder</h1>
            <p class="subtitle">AI Video Generation Platform</p>
        </div>
        
        <div class="error" id="error"></div>
        
        <a href="/auth/login" class="google-btn">
            <svg viewBox="0 0 24 24">
                <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/>
                <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/>
                <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/>
                <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/>
            </svg>
            Sign in with Google
        </a>
        
        <p class="info">Your jobs and data are private to your account</p>
    </div>
    
    <script>
        // Check for error in URL
        const urlParams = new URLSearchParams(window.location.search);
        const error = urlParams.get('error');
        if (error) {
            const errorEl = document.getElementById('error');
            errorEl.textContent = decodeURIComponent(error);
            errorEl.classList.add('show');
        }
    </script>
</body>
</html>
    """)


@app.get("/auth/login")
async def auth_login(request: Request):
    """Initiate Google OAuth flow"""
    if not GOOGLE_AUTH_ENABLED:
        # If auth disabled, just redirect to home
        return RedirectResponse(url="/", status_code=302)
    
    return await handle_google_login(request)


@app.get("/auth/google/callback")
async def auth_callback(request: Request, db: DBSession = Depends(get_db_session)):
    """Handle Google OAuth callback"""
    if not GOOGLE_AUTH_ENABLED:
        return RedirectResponse(url="/", status_code=302)
    
    try:
        user, session_token = await handle_google_callback(request, db)
        
        print(f"[Auth] Cookie set for user {user.email}, token: {session_token[:8]}...", flush=True)
        
        # Return HTML page that sets cookie via JavaScript (more reliable than Set-Cookie on redirects)
        return HTMLResponse(f"""
<!DOCTYPE html>
<html>
<head>
    <title>Logging in...</title>
    <script>
        // Set cookie via JavaScript
        document.cookie = "session={session_token}; path=/; max-age={7 * 24 * 3600}; secure; samesite=lax";
        // Redirect to home
        window.location.href = "/";
    </script>
</head>
<body style="background: #1a1a2e; color: white; font-family: sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0;">
    <div style="text-align: center;">
        <div style="font-size: 24px; margin-bottom: 10px;">🔐</div>
        <div>Logging in...</div>
    </div>
</body>
</html>
""")
        
    except HTTPException as e:
        # Redirect to login with error
        error_msg = str(e.detail)
        return RedirectResponse(url=f"/login?error={error_msg}", status_code=302)
    except Exception as e:
        print(f"[Auth] Callback error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return RedirectResponse(url="/login?error=Authentication failed", status_code=302)


@app.get("/auth/me")
async def auth_me(request: Request, db: DBSession = Depends(get_db_session)):
    """Get current authenticated user info"""
    if not GOOGLE_AUTH_ENABLED:
        # Return default user when auth is disabled
        return {
            "authenticated": True,
            "user": {
                "id": "default",
                "email": "default@local",
                "name": "Default User",
                "picture": None
            }
        }
    
    session_token = request.cookies.get("session")
    if not session_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    from auth import validate_session as db_validate_session
    user = db_validate_session(db, session_token)
    
    if not user:
        raise HTTPException(status_code=401, detail="Session expired")
    
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account disabled")
    
    return {
        "authenticated": True,
        "user": user.to_dict()
    }


@app.post("/auth/logout")
async def auth_logout(request: Request, response: Response, db: DBSession = Depends(get_db_session)):
    """Log out and clear session"""
    session_token = request.cookies.get("session")
    
    if session_token:
        delete_session(db, session_token)
    
    response.delete_cookie("session")
    return {"success": True, "message": "Logged out"}


# ============ User API Keys Management ============

class AddAPIKeyRequest(BaseModel):
    key: str
    name: Optional[str] = None


class AddAPIKeysRequest(BaseModel):
    keys: List[str]  # List of API keys


def validate_single_api_key(api_key: str) -> dict:
    """
    Validate a single API key by testing Veo submission.
    Returns: {"status": "working"|"rate_limited"|"invalid", "message": str}
    """
    VEO_MODEL = "veo-3.1-fast-generate-preview"
    TEST_PROMPT = "A calm blue ocean wave gently rolling onto a sandy beach at sunset"
    
    try:
        from google import genai
        from google.genai import types
        
        client = genai.Client(api_key=api_key)
        
        # Step 1: Quick check if key works at all
        try:
            models = list(client.models.list())
        except Exception as e:
            error_str = str(e).lower()
            if "suspended" in error_str:
                return {"status": "invalid", "message": "Key suspended"}
            elif "invalid" in error_str or "api_key_invalid" in error_str:
                return {"status": "invalid", "message": "Invalid API key"}
            elif "401" in str(e):
                return {"status": "invalid", "message": "Unauthorized"}
            elif "403" in str(e):
                return {"status": "invalid", "message": "Permission denied"}
            else:
                return {"status": "invalid", "message": f"API error: {str(e)[:50]}"}
        
        # Step 2: Try to submit a Veo generation
        config = types.GenerateVideosConfig(
            aspect_ratio="9:16",
            resolution="720p",
            duration_seconds="8",
        )
        
        operation = client.models.generate_videos(
            model=VEO_MODEL,
            prompt=TEST_PROMPT,
            config=config,
        )
        
        # If we get here, the key can submit to Veo!
        return {"status": "working", "message": "Key working"}
        
    except Exception as e:
        error_str = str(e).lower()
        
        if "429" in str(e) or "resource_exhausted" in error_str:
            return {"status": "rate_limited", "message": "Rate limited (quota exhausted)"}
        elif "suspended" in error_str:
            return {"status": "invalid", "message": "Key suspended"}
        elif "permission" in error_str or "403" in str(e):
            return {"status": "invalid", "message": "No Veo access"}
        elif "404" in str(e) or "not found" in error_str:
            return {"status": "invalid", "message": "Veo model not available"}
        else:
            # Unknown error - treat as rate limited to be safe
            return {"status": "rate_limited", "message": f"Error: {str(e)[:40]}"}


@app.get("/api/user/keys")
async def list_user_api_keys(
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """List all API keys for the current user with status summary"""
    keys = db.query(UserAPIKey).filter(
        UserAPIKey.user_id == current_user.id
    ).order_by(UserAPIKey.created_at.desc()).all()
    
    # Calculate summary
    working = sum(1 for k in keys if k.key_status == "working" and k.is_active)
    rate_limited = sum(1 for k in keys if k.key_status == "rate_limited" and k.is_active)
    invalid = sum(1 for k in keys if k.key_status == "invalid" or not k.is_valid)
    inactive = sum(1 for k in keys if not k.is_active)
    
    return {
        "keys": [k.to_dict() for k in keys],
        "count": len(keys),
        "has_keys": len(keys) > 0,
        "summary": {
            "working": working,
            "rate_limited": rate_limited,
            "invalid": invalid,
            "inactive": inactive,
            "total": len(keys),
        }
    }


@app.post("/api/user/keys")
async def add_user_api_key(
    request: AddAPIKeyRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Add a single API key for the current user - validates immediately"""
    key_value = request.key.strip()
    
    # Basic validation
    if not key_value.startswith("AIza"):
        raise HTTPException(status_code=400, detail="Invalid API key format. Gemini keys start with 'AIza'")
    
    if len(key_value) < 30:
        raise HTTPException(status_code=400, detail="API key is too short")
    
    # Check for duplicate
    existing = db.query(UserAPIKey).filter(
        UserAPIKey.user_id == current_user.id,
        UserAPIKey.key_value == key_value
    ).first()
    
    if existing:
        raise HTTPException(status_code=400, detail="This API key is already added")
    
    # Validate the key with Veo API (with error handling)
    print(f"[API Keys] Validating new key ...{key_value[-6:]}", flush=True)
    try:
        validation = validate_single_api_key(key_value)
    except Exception as e:
        print(f"[API Keys] Validation error: {e}", flush=True)
        # If validation fails, still add the key with unknown status
        validation = {"status": "unknown", "message": f"Validation failed: {str(e)[:50]}"}
    
    # Create new key with validation status
    try:
        new_key = UserAPIKey(
            user_id=current_user.id,
            key_value=key_value,
            key_name=request.name,
            key_suffix=key_value[-6:],
            is_valid=(validation["status"] != "invalid"),
            is_active=True,
            key_status=validation["status"],
            last_error=validation["message"] if validation["status"] != "working" else None,
            last_checked=datetime.utcnow(),
        )
        
        db.add(new_key)
        db.commit()
        db.refresh(new_key)
    except Exception as e:
        print(f"[API Keys] Database error: {e}", flush=True)
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)[:100]}")
    
    status_emoji = {"working": "✅", "rate_limited": "⚠️", "invalid": "❌", "unknown": "❓"}.get(validation["status"], "❓")
    
    return {
        "success": True,
        "key": new_key.to_dict(),
        "validation": validation,
        "message": f"{status_emoji} Key added - {validation['message']}"
    }


@app.post("/api/user/keys/bulk")
async def add_user_api_keys_bulk(
    request: AddAPIKeysRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Add multiple API keys at once"""
    added = []
    skipped = []
    errors = []
    
    for i, key_value in enumerate(request.keys):
        key_value = key_value.strip()
        
        # Skip empty lines
        if not key_value:
            continue
        
        # Basic validation
        if not key_value.startswith("AIza"):
            errors.append(f"Key {i+1}: Invalid format (must start with 'AIza')")
            continue
        
        if len(key_value) < 30:
            errors.append(f"Key {i+1}: Too short")
            continue
        
        # Check for duplicate
        existing = db.query(UserAPIKey).filter(
            UserAPIKey.user_id == current_user.id,
            UserAPIKey.key_value == key_value
        ).first()
        
        if existing:
            skipped.append(f"...{key_value[-6:]}")
            continue
        
        # Create new key
        new_key = UserAPIKey(
            user_id=current_user.id,
            key_value=key_value,
            key_suffix=key_value[-6:],
            is_valid=True,
            is_active=True,
        )
        db.add(new_key)
        added.append(f"...{key_value[-6:]}")
    
    db.commit()
    
    return {
        "success": True,
        "added": len(added),
        "skipped": len(skipped),
        "errors": len(errors),
        "details": {
            "added": added,
            "skipped": skipped,
            "errors": errors,
        },
        "message": f"Added {len(added)} keys, skipped {len(skipped)} duplicates"
    }


@app.delete("/api/user/keys/{key_id}")
async def delete_user_api_key(
    key_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Delete a user's API key"""
    key = db.query(UserAPIKey).filter(
        UserAPIKey.id == key_id,
        UserAPIKey.user_id == current_user.id
    ).first()
    
    if not key:
        raise HTTPException(status_code=404, detail="API key not found")
    
    db.delete(key)
    db.commit()
    
    return {"success": True, "message": "API key deleted"}


@app.put("/api/user/keys/{key_id}/toggle")
async def toggle_user_api_key(
    key_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Toggle a user's API key active/inactive"""
    key = db.query(UserAPIKey).filter(
        UserAPIKey.id == key_id,
        UserAPIKey.user_id == current_user.id
    ).first()
    
    if not key:
        raise HTTPException(status_code=404, detail="API key not found")
    
    key.is_active = not key.is_active
    db.commit()
    
    return {
        "success": True,
        "is_active": key.is_active,
        "message": f"API key {'activated' if key.is_active else 'deactivated'}"
    }


@app.post("/api/user/keys/{key_id}/revalidate")
async def revalidate_user_api_key(
    key_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Re-validate a user's API key"""
    key = db.query(UserAPIKey).filter(
        UserAPIKey.id == key_id,
        UserAPIKey.user_id == current_user.id
    ).first()
    
    if not key:
        raise HTTPException(status_code=404, detail="API key not found")
    
    # Validate the key
    print(f"[API Keys] Re-validating key ...{key.key_suffix}", flush=True)
    validation = validate_single_api_key(key.key_value)
    
    # Update status
    key.is_valid = (validation["status"] != "invalid")
    key.key_status = validation["status"]
    key.last_error = validation["message"] if validation["status"] != "working" else None
    key.last_checked = datetime.utcnow()
    db.commit()
    
    status_emoji = {"working": "✅", "rate_limited": "⚠️", "invalid": "❌"}.get(validation["status"], "❓")
    
    return {
        "success": True,
        "key": key.to_dict(),
        "validation": validation,
        "message": f"{status_emoji} {validation['message']}"
    }


@app.post("/api/user/keys/revalidate-all")
async def revalidate_all_user_api_keys(
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Re-validate all of user's API keys"""
    keys = db.query(UserAPIKey).filter(
        UserAPIKey.user_id == current_user.id
    ).all()
    
    if not keys:
        return {"success": True, "message": "No keys to validate", "results": []}
    
    results = []
    for key in keys:
        print(f"[API Keys] Re-validating key ...{key.key_suffix}", flush=True)
        validation = validate_single_api_key(key.key_value)
        
        key.is_valid = (validation["status"] != "invalid")
        key.key_status = validation["status"]
        key.last_error = validation["message"] if validation["status"] != "working" else None
        key.last_checked = datetime.utcnow()
        
        results.append({
            "key_suffix": key.key_suffix,
            "status": validation["status"],
            "message": validation["message"]
        })
    
    db.commit()
    
    working = sum(1 for r in results if r["status"] == "working")
    rate_limited = sum(1 for r in results if r["status"] == "rate_limited")
    invalid = sum(1 for r in results if r["status"] == "invalid")
    
    return {
        "success": True,
        "message": f"Validated {len(keys)} keys: {working} working, {rate_limited} rate-limited, {invalid} invalid",
        "summary": {"working": working, "rate_limited": rate_limited, "invalid": invalid},
        "results": results
    }


@app.delete("/api/user/keys")
async def delete_all_user_api_keys(
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Delete all API keys for the current user"""
    count = db.query(UserAPIKey).filter(
        UserAPIKey.user_id == current_user.id
    ).delete()
    
    db.commit()
    
    return {"success": True, "deleted": count, "message": f"Deleted {count} API keys"}


# ============ Root / UI ============

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main UI"""
    index_path = static_dir / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return HTMLResponse("<h1>Veo Web App</h1><p>UI not found. Place index.html in static/</p>")


# ============ Image Upload ============

@app.post("/api/upload")
async def upload_images(
    files: List[UploadFile] = File(...),
    job_id: Optional[str] = Form(None),
):
    """
    Upload images for video generation.
    Creates a new job directory if job_id not provided.
    Images are renamed sequentially to ensure correct ordering.
    """
    # Create or get job directory
    if job_id is None:
        job_id = str(uuid.uuid4())
    
    job_dir = app_config.uploads_dir / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    
    # Count existing images to continue numbering
    existing_images = [f for f in job_dir.iterdir() if f.suffix.lower() in SUPPORTED_IMAGE_FORMATS]
    next_index = len(existing_images)
    
    uploaded = []
    errors = []
    
    for file in files:
        # Validate file type
        ext = Path(file.filename).suffix.lower()
        if ext not in SUPPORTED_IMAGE_FORMATS:
            errors.append({
                "filename": file.filename,
                "error": f"Unsupported format: {ext}",
                "code": ErrorCode.IMAGE_INVALID_FORMAT.value,
            })
            continue
        
        # Check file size
        content = await file.read()
        if len(content) > MAX_IMAGE_SIZE_BYTES:
            errors.append({
                "filename": file.filename,
                "error": f"File too large: {len(content) / 1024 / 1024:.1f}MB",
                "code": ErrorCode.IMAGE_TOO_LARGE.value,
            })
            continue
        
        # Save file with sequential name to ensure correct ordering
        try:
            # Use sequential naming: image_00.png, image_01.png, etc.
            new_filename = f"image_{next_index:02d}{ext}"
            filepath = job_dir / new_filename
            with open(filepath, "wb") as f:
                f.write(content)
            uploaded.append({
                "filename": new_filename,
                "original_filename": file.filename,
                "size": len(content),
                "path": str(filepath),
                "index": next_index,
            })
            next_index += 1
        except Exception as e:
            errors.append({
                "filename": file.filename,
                "error": str(e),
                "code": ErrorCode.FILE_WRITE_ERROR.value,
            })
    
    return {
        "job_id": job_id,
        "uploaded": uploaded,
        "errors": errors,
        "total_uploaded": len(uploaded),
        "total_errors": len(errors),
    }


@app.get("/api/upload/{job_id}/images")
async def list_uploaded_images(job_id: str):
    """List images uploaded for a job"""
    job_dir = app_config.uploads_dir / job_id
    
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found")
    
    images = []
    for f in job_dir.iterdir():
        if f.suffix.lower() in SUPPORTED_IMAGE_FORMATS:
            images.append({
                "filename": f.name,
                "size": f.stat().st_size,
            })
    
    images.sort(key=lambda x: x["filename"])
    
    return {"job_id": job_id, "images": images, "count": len(images)}


@app.delete("/api/upload/{job_id}")
async def delete_uploaded_images(job_id: str):
    """Delete all uploaded images for a job"""
    job_dir = app_config.uploads_dir / job_id
    
    if job_dir.exists():
        shutil.rmtree(job_dir)
    
    return {"status": "deleted", "job_id": job_id}


@app.delete("/api/upload/{job_id}/image/{filename}")
async def delete_single_image(job_id: str, filename: str):
    """Delete a single uploaded image"""
    job_dir = app_config.uploads_dir / job_id
    
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Upload folder not found")
    
    # Sanitize filename to prevent path traversal
    safe_filename = Path(filename).name
    file_path = job_dir / safe_filename
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Image {safe_filename} not found")
    
    # Delete the file
    file_path.unlink()
    
    # Return remaining images
    remaining = [f.name for f in job_dir.iterdir() if f.suffix.lower() in SUPPORTED_IMAGE_FORMATS]
    remaining.sort()
    
    return {
        "status": "deleted",
        "deleted": safe_filename,
        "remaining": remaining,
        "count": len(remaining)
    }


# ============ Job Management ============

@app.post("/api/jobs", response_model=JobResponse)
async def create_job(
    request: CreateJobRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Create a new video generation job.

    v482: wrapped in try/except so unexpected failures return a
    descriptive error instead of FastAPI's default plain-text
    "Internal Server Error" (which the frontend then failed to
    JSON.parse, producing the confusing "Unexpected token 'I'" alert).
    """
    try:
        return await _create_job_impl(request, db, current_user)
    except HTTPException:
        raise
    except Exception as e:
        import traceback as _tb
        tb_text = _tb.format_exc()
        print(f"[create_job] Unexpected error: {type(e).__name__}: {e}\n{tb_text}", flush=True)
        raise HTTPException(
            500,
            f"Job creation failed: {type(e).__name__}: {str(e) or '(no message)'}"
        )


async def _create_job_impl(
    request: "CreateJobRequest",
    db: "DBSession",
    current_user: "User",
):
    """Actual implementation — wrapped by create_job for error handling."""
    # Use provided job_id (from upload) or generate new one
    job_id = request.job_id if request.job_id else str(uuid.uuid4())
    
    # Validate images exist
    images_dir = app_config.uploads_dir / job_id
    
    if not images_dir.exists() or not any(images_dir.iterdir()):
        raise HTTPException(
            status_code=400,
            detail={"errors": ["No images uploaded. Please upload images first."], "code": ErrorCode.NO_IMAGES.value}
        )
    
    # Create output directory
    output_dir = app_config.outputs_dir / job_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Validate config
    config = request.config
    print(f"[main.py] Received config from UI: language={config.language}, user_context='{config.user_context[:50] if config.user_context else 'empty'}'")
    errors = []
    
    if config.resolution == "1080p" and config.duration != "8":
        errors.append("1080p requires 8 second duration")
    
    if config.use_interpolation and config.duration != "8":
        errors.append("Interpolation requires 8 second duration")
    
    if not request.dialogue_lines:
        errors.append("At least one dialogue line is required")
    
    # Get user's API keys (if any)
    user_keys = db.query(UserAPIKey).filter(
        UserAPIKey.user_id == current_user.id,
        UserAPIKey.is_active == True,
        UserAPIKey.is_valid == True
    ).all()
    
    # --- Get user keys (UI first, then DB) ---
    ui_gemini_keys = [k.strip() for k in (request.api_keys.gemini_keys if request.api_keys else []) if k and k.strip()]
    db_gemini_keys = [k.key_value for k in user_keys] if user_keys else []
    user_effective_keys = ui_gemini_keys or db_gemini_keys
    
    # --- Decide backend BEFORE any fallback ---
    from backends.selector import choose_backend_for_job, BackendType, is_flow_enabled
    
    # Check for user's backend preference from UI toggle
    config_dict_raw = config.model_dump() if hasattr(config, 'model_dump') else {}
    backend_preference = config_dict_raw.get('backend_preference', 'auto')
    
    if backend_preference == 'prompt_only':
        # Prompt Only mode — run the full prompt pipeline but skip generation
        backend = BackendType.FLOW  # Use Flow pipeline for prompt generation
        print(f"[main.py] PROMPT ONLY mode — will generate prompts without submitting", flush=True)
    elif backend_preference == 'api':
        if user_effective_keys:
            backend = BackendType.API
            print(f"[main.py] Backend FORCED to API by user (user keys: {len(user_effective_keys)})", flush=True)
        else:
            errors.append("API backend selected but no API keys configured. Add your Gemini keys in Settings or switch to Flow.")
            backend = BackendType.API
    elif backend_preference == 'flow':
        if is_flow_enabled():
            backend = BackendType.FLOW
            print(f"[main.py] Backend FORCED to FLOW by user", flush=True)
        else:
            errors.append("Flow backend is not available. Set up your worker first or switch to API Keys.")
            backend = BackendType.FLOW
    else:
        backend = choose_backend_for_job(db, current_user.id, user_effective_keys)
        print(f"[main.py] Backend auto-selected: {backend.value} (user keys: {len(user_effective_keys)})", flush=True)
    
    # --- Validate requirements based on backend ---
    if backend == BackendType.API:
        if not user_effective_keys:
            errors.append("No user API keys provided. Add your Gemini keys in Settings.")
        
        if errors:
            raise HTTPException(
                status_code=400,
                detail={"errors": errors, "code": ErrorCode.INVALID_CONFIG.value}
            )
        
        api_keys_data = {
            "gemini_keys": user_effective_keys,             # ONLY user keys
            "openai_key": api_keys_config.openai_api_key    # server-side ok
        }
        print(f"[main.py] API backend: using {len(user_effective_keys)} user keys", flush=True)
    
    elif backend == BackendType.FLOW:
        if not is_flow_enabled():
            errors.append("Flow backend is not configured/enabled on server.")
            raise HTTPException(
                status_code=500,
                detail={"errors": errors, "code": ErrorCode.INVALID_CONFIG.value}
            )
        
        # Clear any non-key-related errors for Flow
        errors = [e for e in errors if "API key" not in e and "Gemini" not in e]
        
        if errors:
            raise HTTPException(
                status_code=400,
                detail={"errors": errors, "code": ErrorCode.INVALID_CONFIG.value}
            )
        
        api_keys_data = {
            "gemini_keys": [],                              # Flow doesn't need Gemini keys
            "openai_key": api_keys_config.openai_api_key
        }
        print(f"[main.py] FLOW backend: no Gemini keys needed", flush=True)
    
    # Create job record
    config_dict = config.model_dump()
    print(f"[main.py] Creating job with config: language={config_dict.get('language')}, user_context='{config_dict.get('user_context', '')[:50] if config_dict.get('user_context') else 'empty'}'")
    
    # Convert dialogue lines to dict, preserving all clip settings
    dialogue_list = [d.model_dump() for d in request.dialogue_lines]
    print(f"[main.py] Dialogue lines with clip settings: {json.dumps(dialogue_list, indent=2)}")
    
    # Convert scenes if provided (storyboard mode)
    scenes_list = None
    if request.scenes:
        scenes_list = [s.model_dump() for s in request.scenes]
        print(f"[main.py] Scenes structure: {json.dumps(scenes_list, indent=2)}")
    
    # Log last frame index if set
    if request.last_frame_index is not None:
        print(f"[main.py] Last frame index: {request.last_frame_index}")
    
    # All jobs start as 'preparing' — background task will set final status
    initial_status = "preparing"
    
    # Determine actual backend string for DB
    backend_str = "prompt_only" if backend_preference == 'prompt_only' else backend.value
    
    job = Job(
        id=job_id,
        user_id=current_user.id,  # Associate job with current user
        status=initial_status,
        config_json=json.dumps(config_dict),
        dialogue_json=json.dumps({
            "lines": dialogue_list, 
            "scenes": scenes_list,
            "last_frame_index": request.last_frame_index
        }),
        api_keys_json=json.dumps(api_keys_data),
        images_dir=str(images_dir),
        output_dir=str(output_dir),
        total_clips=len(request.dialogue_lines),
        backend=backend_str,
    )
    
    db.add(job)
    db.commit()
    db.refresh(job)
    
    # Create Clip rows for each dialogue line (fast — just DB inserts)
    for idx, line in enumerate(dialogue_list):
        line_text = line.get('text', '') if isinstance(line, dict) else str(line)
        clip_mode = line.get('clip_mode', 'blend') if isinstance(line, dict) else 'blend'
        scene_idx = line.get('scene_index', 0) if isinstance(line, dict) else 0
        # v644 — propagate optional audio-padding suffix from the dialogue
        # line. None when the LLM-authored markdown didn't include a
        # `- **pad:**` bullet for this line. Veo prompt builder (in the
        # background task that follows) appends it after the bare line.
        dialogue_pad = line.get('dialogue_pad') if isinstance(line, dict) else None
        # v667/v668 — propagate transformation-video metadata. cut_mode is
        # 'whisper' (default behavior, NULL → whisper) | 'timeline' (skip
        # whisper-VAD; ffmpeg-trim to target_duration_s). target_duration_s
        # comes from frame_anchor_s diffs computed in prepare_batch_for_video.
        # veo_render_duration_s is the ceil_to(target, [4,6,8]) bucket pick.
        cut_mode = line.get('cut_mode') if isinstance(line, dict) else None
        target_duration_s = line.get('target_duration_s') if isinstance(line, dict) else None
        veo_render_duration_s = line.get('veo_render_duration_s') if isinstance(line, dict) else None
        # v681 — text-card / caption denorm onto Clip rows. scene_type
        # 'text_card' makes the video processor skip Veo and render
        # via ffmpeg drawtext at export time.
        caption_val = line.get('caption') if isinstance(line, dict) else None
        scene_type_val = line.get('scene_type') if isinstance(line, dict) else None
        bg_color_val = line.get('bg_color') if isinstance(line, dict) else None
        # v681 — text_card duration overload. When scene_type=text_card
        # AND duration_s is set, store it in target_duration_s (which is
        # otherwise the v667/v668 Veo-trim duration; text_card has no
        # Veo render so the field is unused for that meaning).
        td_v681 = line.get('duration_s') if isinstance(line, dict) else None
        if scene_type_val == 'text_card' and td_v681 is not None:
            target_duration_s = float(td_v681)
        clip = Clip(
            job_id=job_id,
            clip_index=idx,
            dialogue_id=idx + 1,
            dialogue_text=line_text,
            dialogue_pad=dialogue_pad,
            status='preparing',  # Background task will set to pending after prompts are built
            clip_mode=clip_mode,
            scene_index=scene_idx,
            cut_mode=cut_mode,
            target_duration_s=target_duration_s,
            veo_render_duration_s=veo_render_duration_s,
            caption=caption_val,            # v681
            scene_type=scene_type_val,      # v681
            bg_color=bg_color_val,          # v681
        )
        db.add(clip)
    db.commit()
    print(f"[main.py] Created {len(dialogue_list)} clip rows for job {job_id[:8]}")

    # v475: if this job originated from an image batch ("Prepare for video"
    # flow from the image tab), stamp the link on the batch so the sidebar
    # can show the 🎥 promoted badge. Ownership check on user_id prevents
    # cross-user link-writing. Failure here is non-fatal — the job is
    # already created; the worst case is the badge doesn't show for this
    # job, which the user can live with.
    #
    # v484: wrap with rollback on failure. If the UPDATE raises (e.g. the
    # database's promoted_video_job_id column is the legacy INTEGER type
    # and can't accept a UUID string), the SQLAlchemy session goes into a
    # pending-rollback state and every subsequent query on the same
    # session fails with PendingRollbackError. That silently cascades into
    # the ASGI error handler and the entire request returns 500 even
    # though the job was already committed successfully.
    if request.image_batch_id:
        try:
            from image_platform import ImageJobBatch
            batch = db.query(ImageJobBatch).filter(
                ImageJobBatch.id == request.image_batch_id,
                ImageJobBatch.user_id == current_user.id,
            ).first()
            if batch:
                batch.promoted_video_job_id = job_id
                db.commit()
                print(f"[main.py] Stamped batch {request.image_batch_id} with promoted_video_job_id={job_id[:8]}", flush=True)
            else:
                print(f"[main.py] Batch {request.image_batch_id} not found or not owned by user — skipping link", flush=True)
        except Exception as e:
            # Critical: rollback so the session is usable again
            try:
                db.rollback()
            except Exception:
                pass
            print(f"[main.py] Could not stamp batch promotion link (non-fatal): {e}", flush=True)
    
    add_job_log(db, job_id, f"Job created (backend: {backend_str})", "INFO", "system")
    
    # Return immediately — heavy work runs in background
    response = JobResponse(
        id=job.id,
        status=job.status,
        progress_percent=0,
        total_clips=job.total_clips,
        completed_clips=0,
        failed_clips=0,
        skipped_clips=0,
        created_at=job.created_at.isoformat() if job.created_at else None,
        started_at=job.started_at.isoformat() if job.started_at else None,
        completed_at=None,
        backend=job.backend,
        first_dialogue=dialogue_list[0].get('text', '')[:80] if dialogue_list else None,
        first_frame_url=None,
    )
    
    # Spawn background task for frame upload + prompt generation
    import asyncio
    asyncio.create_task(_setup_job_background(
        job_id=job_id,
        images_dir=str(images_dir),
        output_dir=str(output_dir),
        backend_str=backend_str,
        backend_preference=backend_preference,
        config_dict=config_dict,
        dialogue_list=dialogue_list,
        scenes_list=scenes_list,
        last_frame_index=request.last_frame_index,
        api_keys_data=api_keys_data,
    ))
    
    return response


async def _setup_job_background(
    job_id: str,
    images_dir: str,
    output_dir: str,
    backend_str: str,
    backend_preference: str,
    config_dict: dict,
    dialogue_list: list,
    scenes_list: list,
    last_frame_index: int,
    api_keys_data: dict,
):
    """Background task: upload frames to R2, generate prompts, set final job status."""
    from models import SessionLocal
    from pathlib import Path

    db = SessionLocal()
    images_path = Path(images_dir)

    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            print(f"[Background] ERROR: Job {job_id} not found", flush=True)
            return

        print(f"[Background] Starting setup for job {job_id[:8]} (backend={backend_str})", flush=True)

        # === Phase 1: Upload frames to R2 ===
        from backends.storage import is_storage_configured, get_storage

        frames_storage_keys = {}
        first_frame_local_path = None
        upload_errors = []

        if is_storage_configured():
            try:
                storage = get_storage()
                if images_path.exists():
                    image_extensions = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
                    image_files = [f for f in sorted(images_path.iterdir()) if f.suffix.lower() in image_extensions]
                    if image_files:
                        print(f"[Background] Uploading {len(image_files)} frames to R2")
                    for img_file in image_files:
                        try:
                            if not first_frame_local_path:
                                first_frame_local_path = img_file
                            remote_key = await asyncio.to_thread(storage.upload_job_frame, job_id, img_file.name, img_file)
                            frames_storage_keys[img_file.name] = remote_key
                        except Exception as e:
                            upload_errors.append(f"{img_file.name}: {str(e)[:100]}")
                            print(f"[Background] Failed to upload frame {img_file.name}: {e}")

                if frames_storage_keys:
                    job.frames_storage_keys = json.dumps(frames_storage_keys)
                    db.commit()
                    add_job_log(db, job_id, f"✓ Backed up {len(frames_storage_keys)} frames to cloud storage", "INFO", "system")
            except Exception as e:
                print(f"[Background] Storage error: {e}")
                add_job_log(db, job_id, f"⚠️ Cloud storage error: {str(e)[:100]}", "WARNING", "system")
        else:
            if images_path.exists():
                image_extensions = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
                for img_file in sorted(images_path.iterdir()):
                    if img_file.suffix.lower() in image_extensions:
                        first_frame_local_path = img_file
                        break

        # === Phase 2: API jobs — just set pending ===
        is_flow = backend_str == 'flow'
        is_prompt_only = backend_preference == 'prompt_only'

        if not is_flow and not is_prompt_only:
            job.status = JobStatus.PENDING.value
            db.commit()
            add_job_log(db, job_id, "Job ready for processing", "INFO", "system")
            print(f"[Background] API job {job_id[:8]} → pending", flush=True)
            return

        # === Phase 3: Flow/PromptOnly — generate prompts ===
        if is_flow and (not is_storage_configured() or not frames_storage_keys):
            job.status = JobStatus.FAILED.value
            job.error_message = "No frames uploaded to cloud storage for Flow job"
            db.commit()
            add_job_log(db, job_id, "❌ No frames in R2", "ERROR", "flow")
            return

        try:
            from config import VideoConfig
            from veo_generator import (
                build_prompt, analyze_frame, process_user_context,
                get_default_voice_profile, generate_voice_profile,
                generate_transition_cue,
            )
            import tempfile

            language = config_dict.get("language", "English")
            openai_key = os.environ.get("OPENAI_API_KEY")

            video_config = VideoConfig(
                aspect_ratio=config_dict.get("aspect_ratio", "9:16"),
                resolution=config_dict.get("resolution", "720p"),
                duration=config_dict.get("duration", "8"),
                language=language,
                use_interpolation=config_dict.get("use_interpolation", True),
                use_openai_prompt_tuning=config_dict.get("use_openai_prompt_tuning", True),
                use_frame_vision=config_dict.get("use_frame_vision", True),
                single_image_mode=config_dict.get("single_image_mode", True),
                custom_prompt=config_dict.get("custom_prompt", ""),
                user_context=config_dict.get("user_context", ""),
                use_gesture_enrichment=config_dict.get("use_gesture_enrichment", False),
            )

            # v673 — fast lane for fully-prebuilt prompt jobs.
            # When EVERY clip carries a veo_prompt_override, build_prompt
            # short-circuits per clip (veo_generator.py:1358) and never
            # reads frame_analysis / voice_profile / user_context_enriched
            # / transition_cue. Running them anyway burns three OpenAI
            # vision/chat calls + N transition-cue calls per export
            # entirely for log decoration. Skip the whole upstream block.
            _peek_dialogue_data = json.loads(job.dialogue_json) if job.dialogue_json else {}
            _peek_lines = _peek_dialogue_data.get("lines", []) or []
            # v682t — text_card clips bypass Veo entirely (rendered via
            # ffmpeg drawtext at video assembly), so they legitimately
            # have veo_prompt_override=None. Treat them as "doesn't
            # need prebuilt" instead of failing the fast-lane gate.
            # Otherwise a single text_card scene defeats the entire
            # v673 optimization for an otherwise fully-prebuilt batch.
            def _line_satisfies_fast_lane(_l):
                if not isinstance(_l, dict):
                    return False
                if (_l.get("scene_type") or "").lower() == "text_card":
                    return True  # drawtext, no Veo render needed
                return bool((_l.get("veo_prompt_override") or "").strip())
            _all_prebuilt = bool(_peek_lines) and all(
                _line_satisfies_fast_lane(_l) for _l in _peek_lines
            )

            # ── One-time analysis ──
            # Download first frame to local temp for vision analysis
            _first_frame_local = None
            if not _all_prebuilt:
                if first_frame_local_path:
                    _first_frame_local = str(first_frame_local_path)
                elif frames_storage_keys:
                    # Download from R2
                    try:
                        from backends.storage import get_storage
                        _storage = get_storage()
                        _first_key = f"jobs/{job_id}/frames/{sorted(frames_storage_keys.keys())[0]}"
                        _first_frame_local = tempfile.mktemp(suffix='.png')
                        await asyncio.to_thread(_storage.download_file, _first_key, _first_frame_local)
                    except Exception as _dl:
                        print(f"[Background] Could not download first frame for analysis: {_dl}")
                        _first_frame_local = None
            else:
                print(
                    f"[Background] v673 fast lane: all {len(_peek_lines)} clip(s) have "
                    f"prebuilt prompts — skipping first-frame analysis download",
                    flush=True,
                )

            # Process user context → enriched dict
            user_context_enriched = {}
            if not _all_prebuilt and video_config.user_context and openai_key:
                try:
                    print(f"[Background] Analyzing user context...", flush=True)
                    add_job_log(db, job_id, "Analyzing user context...", "INFO", "system")
                    db.commit()
                    user_context_enriched = await asyncio.to_thread(process_user_context, video_config.user_context, language, openai_key)
                    print(f"[Background] ✓ User context analyzed", flush=True)
                except Exception as e:
                    print(f"[Background] Context analysis failed: {e}")

            # Analyze frame → dict
            frame_analysis = {}
            if not _all_prebuilt and _first_frame_local and openai_key and video_config.use_frame_vision:
                try:
                    print(f"[Background] Analyzing frame with vision...", flush=True)
                    add_job_log(db, job_id, "Analyzing frame with AI vision...", "INFO", "system")
                    db.commit()
                    frame_analysis = await asyncio.to_thread(analyze_frame, _first_frame_local, openai_key)
                    print(f"[Background] ✓ Frame analyzed", flush=True)
                except Exception as e:
                    print(f"[Background] Frame analysis failed: {e}")

            # Generate voice profile
            voice_profile = get_default_voice_profile(language, config_dict.get('user_context', ''))
            if not _all_prebuilt and video_config.use_openai_prompt_tuning and openai_key and frame_analysis:
                try:
                    print(f"[Background] Generating voice profile...", flush=True)
                    add_job_log(db, job_id, "Generating voice profile...", "INFO", "system")
                    db.commit()
                    voice_profile = await asyncio.to_thread(generate_voice_profile, frame_analysis, language, user_context_enriched, openai_key)
                    print(f"[Background] ✓ Voice profile generated", flush=True)
                except Exception as e:
                    print(f"[Background] Voice profile failed: {e}")

            # Store analysis in job config for redo
            try:
                stored_config = json.loads(job.config_json) if job.config_json else {}
                stored_config['_voice_profile'] = voice_profile
                stored_config['_frame_analysis'] = frame_analysis
                stored_config['_user_context_enriched'] = user_context_enriched
                job.config_json = json.dumps(stored_config)
                db.commit()
            except Exception:
                pass

            # ── Download all unique frames to local temp for build_prompt ──
            uploaded_frames_list = sorted(frames_storage_keys.keys())
            num_images = len(uploaded_frames_list)
            local_frame_paths = {}  # filename → local temp path
            if frames_storage_keys:
                try:
                    from backends.storage import get_storage
                    _storage = get_storage()
                    for fname in uploaded_frames_list:
                        r2_key = f"jobs/{job_id}/frames/{fname}"
                        local_path = tempfile.mktemp(suffix=Path(fname).suffix)
                        await asyncio.to_thread(_storage.download_file, r2_key, local_path)
                        local_frame_paths[fname] = local_path
                except Exception as _dl:
                    print(f"[Background] Frame download for prompts failed: {_dl}")

            dialogue_data = json.loads(job.dialogue_json) if job.dialogue_json else {}
            dialogue_raw = dialogue_data.get("lines", [])
            total_clips = len(dialogue_raw)
            use_interpolation = video_config.use_interpolation
            duration_val = int(video_config.duration) if video_config.duration else 8

            print(f"[Background] Building prompts for {total_clips} clips ({num_images} frames)", flush=True)
            add_job_log(db, job_id, f"Building prompts for {total_clips} clips...", "INFO", "system")
            db.commit()

            for idx in range(total_clips):
                line_data = dialogue_raw[idx] if isinstance(dialogue_raw[idx], dict) else {"text": dialogue_raw[idx]}
                dialogue_text = line_data.get("text", "")
                clip_mode = line_data.get("clip_mode", "blend")
                action_note = line_data.get("action_note", None)

                # v682h — skip text_card clips entirely. They have no Veo
                # render (rendered via ffmpeg drawtext at video assembly),
                # no start_image_idx (it's null per text_card design), and
                # no dialogue.
                #
                # v688 — additionally MARK the Clip as COMPLETED + auto-
                # approved so the Flow worker doesn't pick it up. Pre-v688
                # the clip stayed at status='preparing' (the initial value
                # from main.py's Clip writer) which apparently wasn't
                # filtered out everywhere — user reported scene 5 (text_card)
                # showing up in the Review & Approve UI as 'REDO QUEUED' /
                # 'Redo submission failed'. Now: clip immediately marks
                # itself as completed with a placeholder prompt so the
                # video assembly's drawtext path takes over without any
                # Flow round-trip.
                if (line_data.get("scene_type") or "").lower() == "text_card":
                    print(f"[Background] Clip {idx}: text_card — skipping Veo prompt build (drawtext at assembly)", flush=True)
                    _tc_clip = db.query(Clip).filter(
                        Clip.job_id == job_id, Clip.clip_index == idx
                    ).first()
                    if _tc_clip:
                        _tc_clip.prompt_text = (
                            f"[text_card placeholder — caption: "
                            f"{(line_data.get('caption') or '').strip()!r}, "
                            f"bg: {(line_data.get('bg_color') or 'black').strip()}, "
                            f"duration: {line_data.get('duration_s') or 1.0}s]"
                        )
                        # Mark complete so Flow worker skips. Auto-approve so
                        # it doesn't sit in pending_review state forever
                        # (drawtext output isn't user-reviewable per-clip;
                        # the final assembled video is the artifact users
                        # review).
                        _tc_clip.status = ClipStatus.COMPLETED.value
                        _tc_clip.approval_status = "approved"
                        # Stamp completed_at so progress calculations include it.
                        if not _tc_clip.completed_at:
                            _tc_clip.completed_at = datetime.utcnow()
                        db.commit()
                        print(f"[Background] Clip {idx}: text_card → marked COMPLETED + approved (drawtext path)", flush=True)
                    continue

                # v682h — handle missing start_image_idx defensively
                # (silent + on-camera scenes always have one, but
                # legacy or malformed payloads might not).
                start_image_idx_raw = line_data.get("start_image_idx")
                if start_image_idx_raw is None:
                    start_image_idx = 0
                else:
                    start_image_idx = start_image_idx_raw
                # v644 — audio-padding suffix appended to the Veo prompt
                # only. Whisper-VAD continues to use the bare `dialogue_text`
                # as script truth, so the pad's spoken audio is trimmed
                # by the existing apply_vad pipeline as unmatched filler.
                _dialogue_pad = (line_data.get("dialogue_pad") or "").strip()
                _padded_dialogue_for_veo = (
                    f"{dialogue_text} {_dialogue_pad}".strip()
                    if _dialogue_pad else dialogue_text
                )
                # v572 — per-clip Veo prompt override. When non-empty,
                # build_prompt short-circuits and ships the prebuilt
                # prompt to Veo verbatim (with the negative-prompt
                # trailer concatenated, if any). Empty string or null
                # = fall through to the auto-build path.
                _veo_prompt_override = (line_data.get("veo_prompt_override") or "").strip() or None
                _veo_negative_override = (line_data.get("veo_negative_prompt_override") or "").strip() or None
                # v537 — explicit speaker_mode from markdown overrides the
                # auto-detection in build_prompt. Empty string or 'auto' →
                # leave as None so build_prompt's _detect_voiceover_only()
                # runs as before. 'on-camera' → False (lip-sync ON).
                # 'voiceover' → True (off-screen narration).
                _speaker_mode = (line_data.get("speaker_mode") or "").strip().lower()
                if _speaker_mode == "on-camera":
                    _voiceover_only_override: Optional[bool] = False
                elif _speaker_mode == "voiceover":
                    _voiceover_only_override = True
                else:
                    _voiceover_only_override = None  # auto / unset

                # Determine start frame
                start_fname = None
                if start_image_idx < num_images:
                    start_fname = uploaded_frames_list[start_image_idx]
                elif num_images > 0:
                    start_fname = uploaded_frames_list[start_image_idx % num_images]

                # End frame logic (same as before — scenes, blend, interpolation)
                end_fname = None
                is_last_clip = (idx == total_clips - 1)
                scenes = dialogue_data.get("scenes", [])

                if scenes:
                    current_scene = None
                    current_scene_idx = 0
                    for si, scene in enumerate(scenes):
                        if idx in scene.get("clips", []):
                            current_scene = scene
                            current_scene_idx = si
                            break
                    if current_scene:
                        scene_clips = sorted(current_scene.get("clips", []))
                        is_last_in_scene = (idx == scene_clips[-1]) if scene_clips else False
                        if is_last_in_scene and not is_last_clip:
                            next_scene_idx = current_scene_idx + 1
                            if next_scene_idx < len(scenes):
                                next_scene = scenes[next_scene_idx]
                                if next_scene.get("transition", "blend") != "cut":
                                    # v682e — text_card scenes have imageIndex=None.
                                    # Use .get() default of None (not 0 — defaulting
                                    # to 0 silently misroutes text_card-following
                                    # interpolation to image 0). Skip end-frame
                                    # interpolation when next is text_card.
                                    next_img = next_scene.get("imageIndex")
                                    if (
                                        next_img is not None
                                        and isinstance(next_img, int)
                                        and 0 <= next_img < num_images
                                    ):
                                        end_fname = uploaded_frames_list[next_img]
                        elif is_last_clip:
                            lfi = dialogue_data.get("last_frame_index")
                            if lfi is not None and lfi < num_images:
                                end_fname = uploaded_frames_list[lfi]
                            elif clip_mode == "blend" and start_image_idx < num_images:
                                end_fname = uploaded_frames_list[start_image_idx]
                        else:
                            if clip_mode == "blend":
                                end_fname = start_fname
                elif num_images == 1 and use_interpolation and clip_mode == "blend":
                    end_fname = start_fname
                elif num_images > 1 and clip_mode == "blend" and use_interpolation:
                    # Multi-image, blend mode, no scenes → self-interpolation
                    end_fname = start_fname

                # R2 keys for DB storage
                start_frame_key = f"jobs/{job_id}/frames/{start_fname}" if start_fname else None
                end_frame_key = f"jobs/{job_id}/frames/{end_fname}" if end_fname else None

                # Local paths for build_prompt
                start_local = Path(local_frame_paths[start_fname]) if start_fname and start_fname in local_frame_paths else None
                end_local = Path(local_frame_paths[end_fname]) if end_fname and end_fname in local_frame_paths else None

                # Generate transition cue if start ≠ end (different frames).
                # v673 — skip when this clip has a prebuilt prompt; build_prompt
                # short-circuits and never reads the cue.
                _transition_cue = None
                if (
                    not _veo_prompt_override
                    and start_local and end_local
                    and str(start_local) != str(end_local)
                    and openai_key
                ):
                    try:
                        _transition_cue = await asyncio.to_thread(
                            generate_transition_cue,
                            str(start_local), str(end_local),
                            dialogue_text, language, float(duration_val), openai_key)
                    except Exception:
                        pass

                # Build prompt using the REAL function signature.
                # v644: send the padded dialogue (line + " " + pad) to Veo
                # so its experimental audio path has enough text to render.
                # The bare line stays in Clip.dialogue_text and is what
                # whisper-VAD matches against; the pad audio gets trimmed
                # automatically as unmatched filler by apply_vad.
                prompt = await asyncio.to_thread(
                    build_prompt,
                    dialogue_line=_padded_dialogue_for_veo,
                    start_frame_path=start_local,
                    end_frame_path=end_local,
                    clip_index=idx,
                    language=language,
                    voice_profile=voice_profile,
                    config=video_config,
                    openai_key=openai_key,
                    frame_analysis=frame_analysis,
                    user_context_override=user_context_enriched if user_context_enriched else None,
                    use_gesture_enrichment=config_dict.get('use_gesture_enrichment', False),
                    transition_cue=_transition_cue,
                    action_note=action_note,
                    short_dialogue_mode=config_dict.get('short_dialogue_mode', 'optimized'),
                    # v539 — prefix-short-lines settings
                    prefix_short_enabled=config_dict.get('prefix_short_enabled', False),
                    prefix_short_word=config_dict.get('prefix_short_word', 'only'),
                    prefix_short_threshold=config_dict.get('prefix_short_threshold', 15),
                    # v537 — pass explicit speaker_mode override. None means
                    # build_prompt falls back to _detect_voiceover_only.
                    voiceover_only=_voiceover_only_override,
                    # v572 — per-clip Veo prompt overrides. When the
                    # text override is non-None, build_prompt skips its
                    # auto-construction logic and returns the prebuilt
                    # prompt verbatim (with negative trailer appended).
                    prebuilt_prompt=_veo_prompt_override,
                    prebuilt_negative_prompt=_veo_negative_override,
                )

                clip = db.query(Clip).filter(Clip.job_id == job_id, Clip.clip_index == idx).first()
                if clip:
                    clip.prompt_text = prompt
                    clip.start_frame = start_frame_key
                    clip.end_frame = end_frame_key
                    clip.status = ClipStatus.PENDING.value

            db.commit()
            print(f"[Background] ✓ All {total_clips} clip prompts committed", flush=True)
            add_job_log(db, job_id, f"✓ All {total_clips} prompts built", "INFO", "system")
            db.commit()

            # Clean up local temp frames
            for _lp in local_frame_paths.values():
                try:
                    os.remove(_lp)
                except Exception:
                    pass
            if _first_frame_local and _first_frame_local not in local_frame_paths.values():
                try:
                    os.remove(_first_frame_local)
                except Exception:
                    pass

            # Clean up local images dir (R2 has them)
            try:
                import shutil
                if images_path.exists():
                    shutil.rmtree(images_path, ignore_errors=True)
            except Exception:
                pass

            # Set final status
            if is_prompt_only:
                job.status = JobStatus.COMPLETED.value
                job.completed_clips = total_clips
                job.progress_percent = 100.0
                job.completed_at = datetime.utcnow()
                db.commit()
                add_job_log(db, job_id, f"✓ Prompt Only: {total_clips} prompts", "INFO", "system")
                print(f"[Background] PromptOnly {job_id[:8]} complete", flush=True)
            else:
                job.status = "queued_for_flow"
                db.commit()
                add_job_log(db, job_id, "Job ready for Flow processing", "INFO", "flow")
                print(f"[Background] Flow {job_id[:8]} → queued_for_flow", flush=True)

        except Exception as e:
            print(f"[Background] Setup failed for {job_id[:8]}: {e}", flush=True)
            import traceback
            traceback.print_exc()
            add_job_log(db, job_id, f"Setup failed: {e}", "ERROR", "system")
            job.status = JobStatus.FAILED.value
            job.error_message = f"Setup failed: {str(e)[:500]}"
            db.commit()

    except Exception as e:
        print(f"[Background] Unexpected error {job_id[:8]}: {e}", flush=True)
        try:
            job = db.query(Job).filter(Job.id == job_id).first()
            if job and job.status == 'preparing':
                job.status = JobStatus.FAILED.value
                job.error_message = f"Setup crashed: {str(e)[:500]}"
                db.commit()
        except Exception:
            pass
    finally:
        db.close()

@app.get("/api/jobs", response_model=List[JobResponse])
async def list_jobs(
    request: Request,
    status: Optional[str] = None,
    limit: int = Query(default=50, le=100),
    offset: int = 0,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """List all jobs for the current user only"""
    query = db.query(Job).filter(
        Job.user_id == current_user.id
    )
    
    if status:
        query = query.filter(Job.status == status)
    
    jobs = query.order_by(Job.created_at.desc()).offset(offset).limit(limit).all()
    
    # Batch-fetch first clip (clip_index=0) for each job to get dialogue + frame
    job_ids = [j.id for j in jobs]
    first_clips = {}
    if job_ids:
        from sqlalchemy import and_
        clips = db.query(Clip).filter(
            and_(Clip.job_id.in_(job_ids), Clip.clip_index == 0)
        ).all()
        for c in clips:
            first_clips[c.job_id] = c
    
    base_url = str(request.base_url).rstrip('/')
    
    result = []
    for j in jobs:
        first_clip = first_clips.get(j.id)
        first_dialogue = None
        first_frame_url = None
        if first_clip:
            first_dialogue = first_clip.dialogue_text[:80] if first_clip.dialogue_text else None
            if first_clip.start_frame:
                fname = first_clip.start_frame.split('/')[-1]
                first_frame_url = f"{base_url}/api/jobs/{j.id}/images/{fname}"
        
        result.append(JobResponse(
            id=j.id,
            status=j.status,
            progress_percent=j.progress_percent,
            total_clips=j.total_clips,
            completed_clips=j.completed_clips,
            failed_clips=j.failed_clips,
            skipped_clips=j.skipped_clips,
            created_at=j.created_at.isoformat() if j.created_at else None,
            started_at=j.started_at.isoformat() if j.started_at else None,
            completed_at=j.completed_at.isoformat() if j.completed_at else None,
            backend=j.backend,
            first_dialogue=first_dialogue,
            first_frame_url=first_frame_url,
            has_export=bool(getattr(j, 'has_export', False)),
            has_voice_clone=bool(getattr(j, 'has_voice_clone', False)),
        ))
    
    return result


def get_user_job(db: DBSession, job_id: str, user: User) -> Job:
    """Helper to get a job and verify ownership"""
    job = db.query(Job).filter(Job.id == job_id).first()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Strict ownership check - user must own the job
    if job.user_id != user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return job


def get_user_clip(db: DBSession, clip_id: int, user: User) -> Clip:
    """Helper to get a clip and verify ownership via job"""
    clip = db.query(Clip).filter(Clip.id == clip_id).first()
    
    if not clip:
        raise HTTPException(status_code=404, detail="Clip not found")
    
    # Verify job ownership - strict check
    job = db.query(Job).filter(Job.id == clip.job_id).first()
    if not job or job.user_id != user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return clip


@app.get("/api/jobs/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Get job details"""
    job = get_user_job(db, job_id, current_user)
    
    return JobResponse(
        id=job.id,
        status=job.status,
        progress_percent=job.progress_percent,
        total_clips=job.total_clips,
        completed_clips=job.completed_clips,
        failed_clips=job.failed_clips,
        skipped_clips=job.skipped_clips,
        created_at=job.created_at.isoformat() if job.created_at else None,
        started_at=job.started_at.isoformat() if job.started_at else None,
        completed_at=job.completed_at.isoformat() if job.completed_at else None,
        backend=job.backend,
    )


@app.get("/api/jobs/{job_id}/config")
async def get_job_config(
    job_id: str, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Get job configuration for cloning - returns config and dialogue data"""
    job = get_user_job(db, job_id, current_user)
    
    # Parse config and dialogue
    config_data = json.loads(job.config_json) if job.config_json else {}
    dialogue_raw = json.loads(job.dialogue_json) if job.dialogue_json else []
    
    # Handle both old format (list) and new format (dict with lines/scenes)
    if isinstance(dialogue_raw, list):
        dialogue_lines = dialogue_raw
        scenes = None
        last_frame_index = None
    else:
        dialogue_lines = dialogue_raw.get("lines", [])
        scenes = dialogue_raw.get("scenes", None)
        last_frame_index = dialogue_raw.get("last_frame_index", None)
    
    # Get list of images - check local filesystem first, then R2
    images = []
    
    # Method 1: Check local filesystem (legacy) - use safe_images_dir helper
    images_path = safe_images_dir(job.images_dir)
    if images_path and images_path.exists():
        # Support all common image formats
        for ext in ["png", "jpg", "jpeg", "webp"]:
            for img_file in sorted(images_path.glob(f"image_*.{ext}")):
                images.append({
                    "filename": img_file.name,
                    "url": f"/api/jobs/{job_id}/images/{img_file.name}"
                })
    
    # Method 2: Check R2 storage if no local images found
    if not images:
        try:
            from backends.storage import is_storage_configured, get_storage
            
            if is_storage_configured():
                storage = get_storage()
                # List images in job's R2 folder
                r2_prefix = f"jobs/{job_id}/frames/"
                r2_keys = storage.list_objects(prefix=r2_prefix, max_keys=100)
                
                for key in sorted(r2_keys):
                    filename = key.split("/")[-1]
                    if filename and any(filename.lower().endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".webp"]):
                        images.append({
                            "filename": filename,
                            "url": f"/api/jobs/{job_id}/images/{filename}"
                        })
                
                if images:
                    print(f"[Config] Found {len(images)} images in R2 for job {job_id}", flush=True)
        except Exception as e:
            print(f"[Config] Error checking R2 for images: {e}", flush=True)
    
    return {
        "job_id": job_id,
        "config": config_data,
        "dialogue_lines": dialogue_lines,
        "scenes": scenes,
        "images": images,
        "images_dir": job.images_dir,
        "last_frame_index": last_frame_index
    }


@app.post("/api/jobs/{src_job_id}/clone-frames")
async def clone_job_frames(
    src_job_id: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v677 — server-side clone of a source job's frames into a fresh upload job.

    Replaces the previous frontend fetch+blob+reupload chain. That chain
    failed for any image-batch-promoted job whose frames lived only on R2:
    /api/jobs/{id}/images/{filename} returns a 302 redirect to a presigned
    R2 URL, and the browser fetch() couldn't reliably read the redirected
    response (CORS / credentials interaction with cross-origin redirect).
    Result: clone overlay stuck on "Loading images..." forever, dialogue
    area never populated, then in v676 the warning surfaced instead.

    This endpoint mirrors POST /api/upload's response shape so the existing
    frontend `uploadedImages` / `uploadedFilesData` plumbing keeps working
    without per-call branching.
    """
    import uuid as _uuid
    import base64 as _b64
    from shutil import copy2

    job = get_user_job(db, src_job_id, current_user)

    new_upload_job_id = str(_uuid.uuid4())
    new_job_dir = app_config.uploads_dir / new_upload_job_id
    new_job_dir.mkdir(parents=True, exist_ok=True)

    _media_types = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp"}

    def _build_data_url(path: Path) -> Optional[str]:
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
            mt = _media_types.get(path.suffix.lower(), "image/png")
            return f"data:{mt};base64," + _b64.b64encode(raw).decode("ascii")
        except Exception as e:
            print(f"[clone-frames] data-url build failed for {path.name}: {e}", flush=True)
            return None

    uploaded: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    next_index = 0

    # Source 1: existing local frames in the source job's images_dir
    src_images_path = safe_images_dir(job.images_dir)
    if src_images_path and src_images_path.exists():
        for ext in (".png", ".jpg", ".jpeg", ".webp"):
            for img_file in sorted(src_images_path.glob(f"image_*{ext}")):
                try:
                    new_filename = f"image_{next_index:02d}{img_file.suffix.lower()}"
                    dst = new_job_dir / new_filename
                    copy2(img_file, dst)
                    uploaded.append({
                        "filename": new_filename,
                        "original_filename": img_file.name,
                        "size": dst.stat().st_size,
                        "path": str(dst),
                        "index": next_index,
                        "data_url": _build_data_url(dst),
                    })
                    next_index += 1
                except Exception as e:
                    errors.append({"filename": img_file.name, "error": str(e)[:200]})

    # Source 2: fall back to R2 when local frames aren't there (typical for
    # any job older than the current container — Render ephemeral disk).
    if not uploaded:
        try:
            from backends.storage import is_storage_configured, get_storage
            if is_storage_configured():
                storage = get_storage()
                r2_prefix = f"jobs/{src_job_id}/frames/"
                r2_keys = storage.list_objects(prefix=r2_prefix, max_keys=200)
                for key in sorted(r2_keys):
                    filename = key.split("/")[-1]
                    if not filename:
                        continue
                    suffix = Path(filename).suffix.lower()
                    if suffix not in (".png", ".jpg", ".jpeg", ".webp"):
                        continue
                    try:
                        new_filename = f"image_{next_index:02d}{suffix}"
                        dst = new_job_dir / new_filename
                        storage.download_file(key, str(dst))
                        if not dst.exists():
                            errors.append({"filename": filename, "error": "R2 download produced no file"})
                            continue
                        uploaded.append({
                            "filename": new_filename,
                            "original_filename": filename,
                            "size": dst.stat().st_size,
                            "path": str(dst),
                            "index": next_index,
                            "data_url": _build_data_url(dst),
                        })
                        next_index += 1
                    except Exception as e:
                        errors.append({"filename": filename, "error": str(e)[:200]})
        except Exception as e:
            print(f"[clone-frames] R2 fallback failed: {e}", flush=True)

    if not uploaded:
        raise HTTPException(
            404,
            f"No frames found for job {src_job_id} (no local files in "
            f"{job.images_dir}, no R2 keys under jobs/{src_job_id}/frames/)"
        )

    print(
        f"[clone-frames] {src_job_id[:8]} → {new_upload_job_id[:8]}: "
        f"copied {len(uploaded)} frame(s)"
        + (f", {len(errors)} error(s)" if errors else ""),
        flush=True,
    )

    return {
        "job_id": new_upload_job_id,
        "uploaded": uploaded,
        "errors": errors,
        "total_uploaded": len(uploaded),
        "total_errors": len(errors),
    }


@app.delete("/api/jobs/{job_id}")
async def delete_job(
    job_id: str, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Delete a job and its data.
    
    v455: if the job is currently claimed by a worker, set abort_requested
    before deleting. The worker sees the flag on its next heartbeat (or
    hits a 404 on its next API call if we delete faster than the heartbeat
    interval) and exits the processing loop cleanly — releases accounts,
    no HOT-marking from spurious failures.
    """
    job = get_user_job(db, job_id, current_user)
    
    # Cancel if running
    if job.status == JobStatus.RUNNING.value:
        worker.cancel_job(job_id)
    
    # Signal abort to the worker before deleting. The flag is written and
    # committed now so any heartbeat in flight sees it. We proceed to
    # delete the row after — the worker's 404 handler catches anything
    # that happens after the delete, also treated as abort.
    if job.claimed_by_worker:
        job.abort_requested = True
        db.commit()
        print(f"[Delete] Job {job_id[:8]} claimed by {job.claimed_by_worker} — signaling abort before delete", flush=True)
    
    # Delete files - use safe_images_dir helper to avoid Path("") issues
    images_dir = safe_images_dir(job.images_dir)
    output_dir = safe_images_dir(job.output_dir)  # Also protect output_dir
    
    if images_dir and images_dir.exists():
        shutil.rmtree(images_dir)
    if output_dir and output_dir.exists():
        shutil.rmtree(output_dir)
    
    # Delete database records
    db.delete(job)
    db.commit()
    
    return {"status": "deleted", "job_id": job_id}


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(
    job_id: str, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Cancel a running job"""
    job = get_user_job(db, job_id, current_user)
    
    # Allow cancel even if status already changed (handle race conditions)
    if job.status not in [JobStatus.RUNNING.value, JobStatus.PENDING.value]:
        # Job already completed/failed/cancelled - just return success
        return {"status": job.status, "job_id": job_id, "message": "Job already finished"}
    
    success = worker.cancel_job(job_id)
    
    if success:
        add_job_log(db, job_id, "Job cancelled by user", "INFO", "system")
        return {"status": "cancelled", "job_id": job_id}
    else:
        raise HTTPException(status_code=500, detail="Failed to cancel job")


@app.post("/api/jobs/{job_id}/pause")
async def pause_job(
    job_id: str, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Pause a running job"""
    job = get_user_job(db, job_id, current_user)
    
    if job.status != JobStatus.RUNNING.value:
        raise HTTPException(status_code=400, detail="Job is not running")
    
    success = worker.pause_job(job_id)
    
    if success:
        add_job_log(db, job_id, "Job paused by user", "INFO", "system")
        return {"status": "paused", "job_id": job_id}
    else:
        raise HTTPException(status_code=500, detail="Failed to pause job")


@app.post("/api/jobs/{job_id}/resume")
async def resume_job(
    job_id: str, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Resume a paused job - reloads user's current API keys"""
    job = get_user_job(db, job_id, current_user)
    
    if job.status != JobStatus.PAUSED.value:
        raise HTTPException(status_code=400, detail="Job is not paused")
    
    # Reload user's current API keys (they may have added new ones)
    user_keys = db.query(UserAPIKey).filter(
        UserAPIKey.user_id == current_user.id,
        UserAPIKey.is_active == True,
        UserAPIKey.is_valid == True
    ).all()
    
    user_gemini_keys = [k.key_value for k in user_keys] if user_keys else []
    
    # Update job with new keys (user's or fallback to server)
    if user_gemini_keys:
        print(f"[Resume] Reloading {len(user_gemini_keys)} user API keys for job {job_id[:8]}", flush=True)
        api_keys_data = {
            "gemini_keys": user_gemini_keys,
            "openai_key": api_keys_config.openai_api_key
        }
    else:
        print(f"[Resume] Using server API keys for job {job_id[:8]}", flush=True)
        api_keys_data = {
            "gemini_keys": api_keys_config.gemini_api_keys,
            "openai_key": api_keys_config.openai_api_key
        }
    
    # Update job with fresh keys
    job.api_keys_json = json.dumps(api_keys_data)
    db.commit()
    
    add_job_log(db, job_id, f"Job resumed with {len(api_keys_data['gemini_keys'])} API keys", "INFO", "system")
    
    success = worker.resume_job(job_id)
    
    if success:
        return {"status": "resumed", "job_id": job_id, "keys_loaded": len(api_keys_data['gemini_keys'])}
    else:
        raise HTTPException(status_code=500, detail="Failed to resume job")


# ============ Clips ============

def deduplicate_versions(versions_json: str) -> list:
    """Deduplicate versions by version_key (attempt.variant), keeping all unique variants"""
    if not versions_json:
        return []
    versions = json.loads(versions_json)
    seen = {}
    for v in versions:
        # Use version_key if available (new format: "1.1", "1.2")
        # Otherwise, fall back to attempt.variant or just attempt
        version_key = v.get("version_key")
        if not version_key:
            attempt = v.get("attempt", 1)
            variant = v.get("variant", 1)
            version_key = f"{attempt}.{variant}"
        
        # Keep the latest entry for each version_key
        seen[version_key] = v
    
    # Sort by attempt, then variant
    return sorted(seen.values(), key=lambda x: (x.get("attempt", 1), x.get("variant", 1)))

def get_actual_versions_count(clip) -> int:
    """Calculate actual number of successful versions for a clip (including all variants)."""
    versions = json.loads(clip.versions_json) if clip.versions_json else []
    
    # Deduplicate by version_key (attempt.variant)
    seen = {}
    for v in versions:
        version_key = v.get("version_key")
        if not version_key:
            attempt = v.get("attempt", 1)
            variant = v.get("variant", 1)
            version_key = f"{attempt}.{variant}"
        seen[version_key] = v
    
    # Add current if completed and not in list
    current_attempt = clip.generation_attempt or 1
    current_key = f"{current_attempt}.1"  # Main output is always variant 1
    if clip.status == ClipStatus.COMPLETED.value and clip.output_filename and current_key not in seen:
        seen[current_key] = {"attempt": current_attempt, "variant": 1, "filename": clip.output_filename}
    
    return len(seen)

@app.get("/api/jobs/{job_id}/clips", response_model=List[ClipResponse])
async def get_job_clips(
    job_id: str, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Get all clips for a job. Returns in lineup order if a custom lineup exists."""
    job = get_user_job(db, job_id, current_user)
    
    all_clips = db.query(Clip).filter(Clip.job_id == job_id).order_by(Clip.clip_index).all()
    
    # If custom lineup exists, return clips in that order
    lineup_set = None
    if job.clip_order_json:
        try:
            lineup_ids = json.loads(job.clip_order_json)
            lineup_set = set(lineup_ids)
            clip_map = {c.id: c for c in all_clips}
            ordered = [clip_map[cid] for cid in lineup_ids if cid in clip_map]
            # Append any clips not in the lineup (e.g. newly generated redos, generating clips)
            for c in all_clips:
                if c.id not in lineup_set:
                    ordered.append(c)
            clips = ordered
        except (json.JSONDecodeError, KeyError):
            clips = all_clips
    else:
        clips = all_clips
    
    return [
        ClipResponse(
            id=c.id,
            clip_index=c.clip_index,
            dialogue_id=c.dialogue_id,
            dialogue_text=c.dialogue_text,
            status=c.status,
            retry_count=c.retry_count,
            start_frame=c.start_frame,
            end_frame=c.end_frame,
            output_filename=c.output_filename,
            error_code=c.error_code,
            error_message=c.error_message,
            approval_status=c.approval_status or "pending_review",
            generation_attempt=c.generation_attempt or 1,
            attempts_remaining=3 - (c.generation_attempt or 1),
            redo_reason=c.redo_reason,
            versions=deduplicate_versions(c.versions_json),
            selected_variant=c.selected_variant if c.selected_variant else 1,
            total_variants=get_actual_versions_count(c),
            clip_mode=c.clip_mode or "blend",
            scene_index=c.scene_index or 0,
            prompt_text=c.prompt_text or None,
            in_lineup=c.id in lineup_set if lineup_set else True,
        )
        for c in clips
    ]


# ============ Clip Review & Approval ============

@app.post("/api/clips/{clip_id}/approve", response_model=ApprovalResponse)
async def approve_clip(
    clip_id: int, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Approve a clip - marks it as accepted by the user.
    For 'continue' mode scenes, this allows the next clip to start generating.
    """
    clip = get_user_clip(db, clip_id, current_user)
    
    if clip.status != ClipStatus.COMPLETED.value:
        raise HTTPException(status_code=400, detail="Can only approve completed clips")
    
    if clip.approval_status == "max_attempts":
        raise HTTPException(status_code=400, detail="Clip has reached max attempts - contact support")
    
    # Update approval status
    clip.approval_status = "approved"
    
    # Update versions history
    versions = json.loads(clip.versions_json) if clip.versions_json else []
    for v in versions:
        if v.get("attempt") == clip.generation_attempt:
            v["approved"] = True
    clip.versions_json = json.dumps(versions)
    
    db.commit()
    
    add_job_log(db, clip.job_id, f"Clip {clip.clip_index + 1} approved by user", "INFO", "approval")
    
    # Auto-append to custom lineup if one exists
    job = clip.job
    if job.clip_order_json:
        try:
            lineup_ids = json.loads(job.clip_order_json)
            if clip.id not in lineup_ids:
                lineup_ids.append(clip.id)
                job.clip_order_json = json.dumps(lineup_ids)
                db.commit()
                add_job_log(db, clip.job_id, f"Clip {clip.clip_index + 1} auto-added to lineup", "INFO", "lineup")
        except Exception:
            pass
    
    # Check if there's a next clip waiting for this approval (continue mode)
    next_clip = db.query(Clip).filter(
        Clip.job_id == clip.job_id,
        Clip.clip_index == clip.clip_index + 1
    ).first()
    
    next_clip_triggered = False
    if next_clip and next_clip.status == ClipStatus.WAITING_APPROVAL.value:
        # Update next clip to PENDING so worker will pick it up
        next_clip.status = ClipStatus.PENDING.value
        db.commit()
        add_job_log(db, clip.job_id, f"Clip {clip.clip_index + 2} now pending (was waiting for clip {clip.clip_index + 1} approval)", "INFO", "approval")
        next_clip_triggered = True
    
    return ApprovalResponse(
        clip_id=clip.id,
        status="approved",
        message="Clip approved" + (" - next clip will start generating" if next_clip_triggered else ""),
        attempts_remaining=3 - clip.generation_attempt
    )


@app.post("/api/clips/{clip_id}/reject", response_model=ApprovalResponse)
async def reject_clip(
    clip_id: int, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Reject a clip without triggering redo.
    User can later choose to redo or leave as rejected.
    """
    clip = get_user_clip(db, clip_id, current_user)
    
    if clip.status != ClipStatus.COMPLETED.value:
        raise HTTPException(status_code=400, detail="Can only reject completed clips")
    
    clip.approval_status = "rejected"
    db.commit()
    
    add_job_log(db, clip.job_id, f"Clip {clip.clip_index + 1} rejected by user", "INFO", "approval")
    
    return ApprovalResponse(
        clip_id=clip.id,
        status="rejected",
        message="Clip has been rejected. You can redo it or leave as is.",
        attempts_remaining=3 - clip.generation_attempt
    )


@app.delete("/api/clips/{clip_id}")
async def delete_clip(
    clip_id: int, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Delete a clip and its video file.
    """
    clip = get_user_clip(db, clip_id, current_user)
    
    job_id = clip.job_id
    clip_index = clip.clip_index
    
    # Delete video file if exists
    if clip.output_filename:
        try:
            job = db.query(Job).filter(Job.id == job_id).first()
            if job and job.output_dir:
                video_path = Path(job.output_dir) / clip.output_filename
                if video_path.exists():
                    video_path.unlink()
        except Exception as e:
            print(f"Error deleting video file: {e}", flush=True)
    
    # Delete from database
    db.delete(clip)
    db.commit()
    
    # Update job stats
    job = db.query(Job).filter(Job.id == job_id).first()
    if job:
        remaining_clips = db.query(Clip).filter(Clip.job_id == job_id).count()
        job.total_clips = remaining_clips
        completed = db.query(Clip).filter(Clip.job_id == job_id, Clip.status == ClipStatus.COMPLETED.value).count()
        job.completed_clips = completed
        if remaining_clips > 0:
            job.progress_percent = int((completed / remaining_clips) * 100)
        db.commit()
    
    add_job_log(db, job_id, f"Clip {clip_index + 1} deleted by user", "INFO", "deletion")
    
    return {"success": True, "message": f"Clip {clip_index + 1} deleted"}


# ============ Lineup Management (Post-Production) ============

class LineupUpdateRequest(BaseModel):
    clip_ids: List[int]  # Ordered list of clip IDs


def _get_lineup_clips(db, job: Job, current_user) -> list:
    """Get clips in lineup order (respects clip_order_json override)."""
    all_clips = db.query(Clip).filter(Clip.job_id == job.id).all()
    clip_map = {c.id: c for c in all_clips}
    
    if job.clip_order_json:
        try:
            order = json.loads(job.clip_order_json)
            # Return clips in specified order, skip missing IDs
            return [clip_map[cid] for cid in order if cid in clip_map]
        except (json.JSONDecodeError, KeyError):
            pass
    
    # Default: approved clips ordered by clip_index
    return sorted(
        [c for c in all_clips if c.approval_status == "approved"],
        key=lambda c: c.clip_index
    )


@app.get("/api/jobs/{job_id}/lineup")
async def get_lineup(
    job_id: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Get the current clip lineup (export order). Respects clip_order_json override."""
    job = get_user_job(db, job_id, current_user)
    
    clips = _get_lineup_clips(db, job, current_user)
    
    # Also return all clips (including removed ones) so the UI can offer re-adding
    all_clips = db.query(Clip).filter(
        Clip.job_id == job_id,
        Clip.status == ClipStatus.COMPLETED.value
    ).order_by(Clip.clip_index).all()
    
    lineup_ids = [c.id for c in clips]
    excluded = [c for c in all_clips if c.id not in lineup_ids]
    
    return {
        "has_override": job.clip_order_json is not None,
        "lineup": [
            {
                "id": c.id,
                "clip_index": c.clip_index,
                "dialogue_text": c.dialogue_text[:100] + "..." if len(c.dialogue_text) > 100 else c.dialogue_text,
                "output_filename": c.output_filename,
                "output_url": c.output_url,
                "approval_status": c.approval_status,
                "selected_variant": c.selected_variant or 1,
                "versions": json.loads(c.versions_json) if c.versions_json else [],
            }
            for c in clips
        ],
        "excluded": [
            {
                "id": c.id,
                "clip_index": c.clip_index,
                "dialogue_text": c.dialogue_text[:100] + "..." if len(c.dialogue_text) > 100 else c.dialogue_text,
                "output_filename": c.output_filename,
                "output_url": c.output_url,
            }
            for c in excluded
        ],
    }


@app.put("/api/jobs/{job_id}/lineup")
async def update_lineup(
    job_id: str,
    request: LineupUpdateRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Set the clip order for export. clip_ids is an ordered list of clip IDs."""
    job = get_user_job(db, job_id, current_user)
    
    # Validate all clip IDs belong to this job
    valid_ids = set(
        c.id for c in db.query(Clip.id).filter(Clip.job_id == job_id).all()
    )
    invalid = [cid for cid in request.clip_ids if cid not in valid_ids]
    if invalid:
        raise HTTPException(status_code=400, detail=f"Invalid clip IDs: {invalid}")
    
    job.clip_order_json = json.dumps(request.clip_ids)
    db.commit()
    
    add_job_log(db, job_id, f"Lineup updated: {len(request.clip_ids)} clips in custom order", "INFO", "lineup")
    
    return {"success": True, "clip_count": len(request.clip_ids)}


@app.delete("/api/jobs/{job_id}/lineup")
async def reset_lineup(
    job_id: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Reset lineup to default order (ORDER BY clip_index, approved only)."""
    job = get_user_job(db, job_id, current_user)
    
    job.clip_order_json = None
    db.commit()
    
    add_job_log(db, job_id, "Lineup reset to default order", "INFO", "lineup")
    
    return {"success": True, "message": "Lineup reset to default order"}


@app.post("/api/jobs/{job_id}/lineup/upload")
async def upload_clip_to_lineup(
    job_id: str,
    video: UploadFile = File(...),
    position: int = Form(-1),  # -1 = append to end
    dialogue_text: str = Form("(uploaded clip)"),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Upload a video file and add it to the lineup as a completed+approved clip.
    
    v527: previously held the dep-injected DB session through a 5-30s
    R2 upload. Under parallel uploads that exhausted the connection
    pool. Now: query needed data, release session, do R2 work, reopen
    session for DB writes.
    """
    job = get_user_job(db, job_id, current_user)
    
    # Validate file type
    if not video.filename.lower().endswith(('.mp4', '.webm', '.mov')):
        raise HTTPException(status_code=400, detail="Only .mp4, .webm, .mov files are supported")
    
    # Read file
    content = await video.read()
    if len(content) > 100 * 1024 * 1024:  # 100MB limit
        raise HTTPException(status_code=400, detail="File too large (max 100MB)")
    
    # Determine next clip_index
    max_idx = db.query(Clip.clip_index).filter(Clip.job_id == job_id).order_by(Clip.clip_index.desc()).first()
    next_idx = (max_idx[0] + 1) if max_idx else 0
    
    # Capture what we need for the I/O phase, then RELEASE the session
    output_dir_str = job.output_dir
    user_id_for_check = current_user.id  # noqa — kept for potential reuse
    
    # v527: release the dep-injected session BEFORE the slow R2 upload.
    # The dep wrapper's finally block will call db.close() again — that's
    # a no-op after the first close.
    db.close()
    
    # ── Slow I/O phase (no DB held) ──
    output_dir = Path(output_dir_str)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    safe_name = f"uploaded_{next_idx}_{uuid.uuid4().hex[:6]}.mp4"
    local_path = output_dir / safe_name
    
    with open(local_path, "wb") as f:
        f.write(content)
    
    # Upload to R2
    from backends.storage import is_storage_configured, get_storage
    if is_storage_configured():
        try:
            storage = get_storage()
            r2_key = f"jobs/{job_id}/outputs/{safe_name}"
            await asyncio.to_thread(storage.upload_file, str(local_path), r2_key, 'video/mp4')
            print(f"[Lineup] Uploaded {safe_name} to R2: {r2_key}", flush=True)
        except Exception as e:
            print(f"[Lineup] R2 upload warning: {e}", flush=True)
    
    # ── Reopen DB for writes ──
    from models import get_db
    with get_db() as db2:
        # Re-fetch job (the dep session is closed)
        job2 = db2.query(Job).filter(Job.id == job_id, Job.user_id == current_user.id).first()
        if not job2:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Create clip row
        clip = Clip(
            job_id=job_id,
            clip_index=next_idx,
            dialogue_id=next_idx + 1,
            dialogue_text=dialogue_text,
            status=ClipStatus.COMPLETED.value,
            approval_status="approved",
            output_filename=safe_name,
            generation_attempt=1,
            versions_json=json.dumps([{
                "attempt": 1,
                "variant": 1,
                "version_key": "1.1",
                "filename": safe_name,
                "generated_at": datetime.utcnow().isoformat(),
                "approved": True,
            }]),
            selected_variant=1,
        )
        db2.add(clip)
        db2.flush()  # Get clip.id
        
        new_clip_id = clip.id
        
        # Update job total_clips
        job2.total_clips = db2.query(Clip).filter(Clip.job_id == job_id).count()
        job2.completed_clips = db2.query(Clip).filter(
            Clip.job_id == job_id, Clip.status == ClipStatus.COMPLETED.value
        ).count()
        
        # Update lineup order: insert at position or append
        current_order = json.loads(job2.clip_order_json) if job2.clip_order_json else None
        if current_order is None:
            # Initialize from current approved clips in default order
            approved = db2.query(Clip.id).filter(
                Clip.job_id == job_id,
                Clip.approval_status == "approved",
                Clip.id != new_clip_id,
            ).order_by(Clip.clip_index).all()
            current_order = [c[0] for c in approved]
        
        if position < 0 or position >= len(current_order):
            current_order.append(new_clip_id)
        else:
            current_order.insert(position, new_clip_id)
        
        job2.clip_order_json = json.dumps(current_order)
        db2.commit()
        
        add_job_log(db2, job_id, f"Uploaded clip added to lineup: {safe_name} at position {position}", "INFO", "lineup")
        db2.commit()
    
    return {
        "success": True,
        "clip_id": new_clip_id,
        "filename": safe_name,
        "position": current_order.index(new_clip_id),
        "lineup_count": len(current_order),
    }


@app.post("/api/clips/{clip_id}/cancel")
async def cancel_clip(
    clip_id: int, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Cancel/skip a clip — marks it as skipped regardless of current state.
    Works on pending, generating, redo_queued, waiting_approval, preparing clips.
    Does NOT work on already-completed or already-approved clips.
    """
    clip = get_user_clip(db, clip_id, current_user)
    
    # Don't allow cancelling completed/approved clips
    if clip.status == ClipStatus.COMPLETED.value and clip.approval_status == "approved":
        raise HTTPException(status_code=400, detail="Cannot cancel an approved clip")
    
    old_status = clip.status
    clip.status = ClipStatus.SKIPPED.value
    clip.error_message = "Skipped by user"
    clip.error_code = "USER_CANCELLED"
    
    job_id = clip.job_id
    clip_index = clip.clip_index
    db.commit()
    
    # Update job stats
    job = db.query(Job).filter(Job.id == job_id).first()
    if job:
        clips = db.query(Clip).filter(Clip.job_id == job_id).all()
        completed = sum(1 for c in clips if c.status == ClipStatus.COMPLETED.value)
        failed = sum(1 for c in clips if c.status == ClipStatus.FAILED.value)
        skipped = sum(1 for c in clips if c.status == ClipStatus.SKIPPED.value)
        total = len(clips)
        job.completed_clips = completed
        job.failed_clips = failed
        job.skipped_clips = skipped
        if total > 0:
            job.progress_percent = int(((completed + failed + skipped) / total) * 100)
        # If all clips are now terminal, mark job completed
        active = sum(1 for c in clips if c.status not in (
            ClipStatus.COMPLETED.value, ClipStatus.FAILED.value, ClipStatus.SKIPPED.value
        ))
        if active == 0 and completed > 0:
            job.status = "completed"
        db.commit()
    
    add_job_log(db, job_id, f"Clip {clip_index + 1} cancelled by user (was: {old_status})", "INFO", "cancellation")
    
    return {"success": True, "message": f"Clip {clip_index + 1} cancelled"}


@app.post("/api/clips/{clip_id}/cancel-redo")
async def cancel_redo_clip(
    clip_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Cancel a queued redo and revert clip to its previous completed version.
    Only works on clips in redo_queued or flow_redo_queued status.

    v468: if there's no previous version to revert to (because the clip's
    original generation also failed — common in the zombie-redo case),
    instead of throwing 400 and leaving the clip stuck forever, mark it
    as failed. User can then delete or fresh-retry. Fixes the stuck-loop
    where a failed-clip redo got stuck in flow_redo_queued and neither
    Retry nor Revert worked.
    """
    clip = get_user_clip(db, clip_id, current_user)
    
    if clip.status not in ('redo_queued', 'flow_redo_queued', 'generating'):
        raise HTTPException(status_code=400, detail=f"Clip is not in redo state (status: {clip.status})")
    
    # Find last version with a filename
    versions = json.loads(clip.versions_json) if clip.versions_json else []
    last_version = None
    for v in reversed(versions):
        if v.get('filename'):
            last_version = v
            break
    
    if not last_version:
        # v468: no prior successful version exists (the original
        # generation must have failed). Instead of leaving the clip
        # wedged in redo-queued forever, mark it failed. The user can
        # then delete it or click redo again to start fresh from whatever
        # state they want.
        clip.status = ClipStatus.FAILED.value
        clip.approval_status = "pending_review"
        clip.error_code = "REDO_STUCK"
        clip.error_message = "Redo could not complete and no prior version exists to revert to. Click Retry to try again, or remove the clip."
        clip.claimed_by_worker = None
        clip.claimed_at = None
        db.commit()
        add_job_log(db, clip.job_id, f"Clip {clip.clip_index + 1} redo cancelled — no prior version to revert to, marked failed", "WARN", "redo_cancel")
        return {
            "success": True,
            "message": "Redo cancelled. Clip marked as failed (no prior version to revert to — you can retry or delete it).",
            "filename": None,
            "status": "failed",
        }
    
    # Restore clip to completed state with previous version
    clip.status = ClipStatus.COMPLETED.value
    clip.approval_status = "pending_review"
    clip.output_filename = last_version['filename']
    clip.output_url = last_version.get('url')
    clip.generation_attempt = last_version.get('attempt', clip.generation_attempt - 1)
    clip.selected_variant = len(versions)  # Select the last version
    clip.error_code = None
    clip.error_message = None
    clip.claimed_by_worker = None
    clip.claimed_at = None
    
    db.commit()
    
    add_job_log(db, clip.job_id, f"Clip {clip.clip_index + 1} redo cancelled — reverted to {last_version['filename']}", "INFO", "redo_cancel")
    
    return {"success": True, "message": f"Reverted to previous version", "filename": last_version['filename']}


@app.post("/api/clips/{clip_id}/select-variant/{variant_num}")
async def select_clip_variant(
    clip_id: int, 
    variant_num: int, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Select a specific variant for a clip.
    variant_num is 1-indexed position in the versions list.
    Handles both old format (attempt only) and new format (attempt.variant).
    Updates output_filename to point to the selected variant's video.
    """
    clip = get_user_clip(db, clip_id, current_user)
    
    versions = json.loads(clip.versions_json) if clip.versions_json else []
    
    # Add current version if it's completed and not already in the list
    current_attempt = clip.generation_attempt or 1
    
    # Check if current output is already in versions BY FILENAME (not by attempt/variant combo)
    already_exists = False
    for v in versions:
        if v.get("filename") == clip.output_filename:
            already_exists = True
            break
    
    if clip.status == ClipStatus.COMPLETED.value and clip.output_filename and not already_exists:
        # Add as new format entry
        versions.append({
            "attempt": current_attempt,
            "variant": 1,
            "version_key": f"{current_attempt}.1",
            "filename": clip.output_filename,
            "url": clip.output_url,
            "generated_at": clip.completed_at.isoformat() if clip.completed_at else None,
            "approved": clip.approval_status == "approved",
            "start_frame": clip.start_frame,
            "end_frame": clip.end_frame,
        })
    
    # Sort versions by attempt, then variant
    versions.sort(key=lambda x: (x.get("attempt", 1), x.get("variant", 1)))
    
    # Save cleaned versions back
    clip.versions_json = json.dumps(versions)
    
    if not versions:
        raise HTTPException(status_code=400, detail="No variants available")
    
    # Check variant is in valid range (1-indexed position)
    if variant_num < 1 or variant_num > len(versions):
        raise HTTPException(status_code=400, detail=f"Variant must be between 1 and {len(versions)}")
    
    # Get variant by position (1-indexed)
    variant = versions[variant_num - 1]
    
    if not variant or not variant.get("filename"):
        raise HTTPException(status_code=404, detail=f"Variant {variant_num} has no video file")
    
    # Update selected variant and output filename
    clip.selected_variant = variant_num  # Store position
    clip.output_filename = variant.get("filename")
    if variant.get("url"):
        clip.output_url = variant.get("url")
    clip.approval_status = "pending_review"  # Reset approval when switching
    db.commit()
    
    add_job_log(db, clip.job_id, f"Clip {clip.clip_index + 1} switched to variant {variant_num}", "INFO", "variant")
    
    # Return full clip data for UI update
    return {
        "success": True,
        "selected_variant": variant_num,
        "filename": variant.get("filename"),
        "total_variants": len(versions),
        "clip": ClipResponse(
            id=clip.id,
            clip_index=clip.clip_index,
            dialogue_id=clip.dialogue_id or 0,
            dialogue_text=clip.dialogue_text or "",
            status=clip.status,
            retry_count=clip.retry_count or 0,
            start_frame=clip.start_frame,
            end_frame=clip.end_frame,
            output_filename=clip.output_filename,
            error_code=clip.error_code,
            error_message=clip.error_message,
            approval_status=clip.approval_status or "pending_review",
            generation_attempt=clip.generation_attempt or 1,
            attempts_remaining=3 - (clip.generation_attempt or 1),
            redo_reason=clip.redo_reason,
            selected_variant=variant_num,
            total_variants=len(versions),
            versions=versions if versions else []
        )
    }


@app.post("/api/clips/{clip_id}/redo", response_model=ApprovalResponse)
async def request_clip_redo(
    clip_id: int, 
    request: RedoRequest = None,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Request a redo for a clip.
    
    - Attempt 1 → 2: Uses same logged parameters
    - Attempt 2 → 3: Uses fresh parameters (no log)
    - Attempt 3: No more redos allowed, must contact support
    
    For Flow backend jobs: sets status to 'flow_redo_queued' (handled by Flow worker)
    For API backend jobs: sets status to 'redo_queued' (handled by API worker)
    """
    clip = get_user_clip(db, clip_id, current_user)
    
    # Get the job early for backend detection
    job = db.query(Job).filter(Job.id == clip.job_id).first()
    is_flow = job and job.backend == 'flow'
    
    # Check if already queued or generating - prevent duplicate requests
    # Accept both redo_queued and flow_redo_queued as "already queued"
    if clip.status in [ClipStatus.REDO_QUEUED.value, ClipStatus.FLOW_REDO_QUEUED.value]:
        return ApprovalResponse(
            clip_id=clip.id,
            status="redo_queued",
            message="Redo already queued - please wait",
            attempts_remaining=3 - clip.generation_attempt
        )
    
    if clip.status == ClipStatus.GENERATING.value:
        raise HTTPException(status_code=400, detail="Clip is currently generating - please wait")
    
    if clip.status == ClipStatus.PENDING.value:
        raise HTTPException(status_code=400, detail="Clip is pending initial generation")
    
    # Allow redo for completed or failed clips
    if clip.status not in [ClipStatus.COMPLETED.value, ClipStatus.FAILED.value]:
        raise HTTPException(status_code=400, detail=f"Can only redo completed or failed clips (current status: {clip.status})")
    
    # Check attempt limit
    if clip.generation_attempt >= 3:
        clip.approval_status = "max_attempts"
        db.commit()
        raise HTTPException(
            status_code=400, 
            detail={
                "code": "MAX_ATTEMPTS_REACHED",
                "message": "Maximum 3 attempts reached. Please contact support for assistance.",
                "support_email": "support@yourdomain.com"
            }
        )
    
    # Save current version to history before redo (avoid duplicates)
    versions = json.loads(clip.versions_json) if clip.versions_json else []
    existing_attempts = [v.get('attempt') for v in versions]
    
    # Only add if this attempt isn't already saved (avoid duplicates from worker)
    if clip.generation_attempt not in existing_attempts and clip.output_filename:
        versions.append({
            "attempt": clip.generation_attempt,
            "filename": clip.output_filename,
            "generated_at": clip.completed_at.isoformat() if clip.completed_at else None,
            "approved": False,
            "start_frame": clip.start_frame,
            "end_frame": clip.end_frame,
        })
        clip.versions_json = json.dumps(versions)
    
    # Increment attempt
    new_attempt = clip.generation_attempt + 1
    clip.generation_attempt = new_attempt
    
    # Determine if we use logged params
    # Attempt 2: use logged params (same settings)
    # Attempt 3: fresh generation (no logged params)
    clip.use_logged_params = (new_attempt == 2)
    
    # Set status for redo queue based on backend type
    # This is the KEY SEPARATION: Flow worker only sees flow_redo_queued, API worker only sees redo_queued
    if is_flow:
        clip.status = ClipStatus.FLOW_REDO_QUEUED.value
        status_for_response = "flow_redo_queued"
    else:
        clip.status = ClipStatus.REDO_QUEUED.value
        status_for_response = "redo_queued"
    
    clip.approval_status = "rejected"
    clip.redo_reason = request.reason if request else None
    
    # Clear worker claim fields so the appropriate worker can claim again
    clip.claimed_by_worker = None
    clip.claimed_at = None
    
    # Update dialogue if provided
    if request and request.new_dialogue is not None:
        clip.dialogue_text = request.new_dialogue.strip()
        add_job_log(
            db, clip.job_id,
            f"Clip {clip.clip_index + 1} dialogue updated for redo",
            "INFO", "approval",
            details={"new_dialogue": clip.dialogue_text}
        )
    
    # Clear previous output (keep in versions history)
    clip.output_filename = None
    clip.error_code = None
    clip.error_message = None

    # v485: bump job.updated_at. SQLAlchemy's onupdate=datetime.utcnow on
    # Job.updated_at fires only when a column on the Job ROW itself
    # changes. Modifying a child Clip doesn't propagate. Without this
    # explicit bump, the worker's redo-pending query
    #   Job.updated_at >= now() - 24h
    # excludes old jobs and the redo gets orphaned — worker keeps polling
    # and reports "No pending jobs or redos" forever despite the redo
    # sitting in flow_redo_queued. Touching the job row here ensures the
    # 24h activity window covers anything the user is actively iterating.
    job.updated_at = datetime.utcnow()
    
    # === REGENERATE PROMPT with feedback + new dialogue baked in ===
    # Instead of just prepending feedback, rebuild the prompt from scratch so
    # gesture cues, transition cues, and object rules all match the new dialogue.
    try:
        from veo_generator import (
            build_prompt, generate_transition_cue,
            get_default_voice_profile,
        )
        from config import VideoConfig
        
        config_data = json.loads(job.config_json) if job.config_json else {}
        language = config_data.get('language', 'English')
        voice_profile = config_data.get('_voice_profile', '')
        frame_analysis = config_data.get('_frame_analysis', {})
        user_context_enriched = config_data.get('_user_context_enriched', {})
        openai_key = None
        
        # Get OpenAI key from job's api_keys
        try:
            api_keys = json.loads(job.api_keys_json) if job.api_keys_json else {}
            openai_key = api_keys.get('openai_key') or os.environ.get('OPENAI_API_KEY')
        except Exception:
            openai_key = os.environ.get('OPENAI_API_KEY')
        
        if not voice_profile:
            voice_profile = get_default_voice_profile(language, config_data.get('user_context', ''))
        
        video_config = VideoConfig(
            language=language,
            duration=config_data.get('duration', '8'),
            use_openai_prompt_tuning=config_data.get('use_openai_prompt_tuning', True),
            use_gesture_enrichment=config_data.get('use_gesture_enrichment', False),
        )
        
        # Download frames from R2 to temp for vision-based cue generation
        _start_local = None
        _end_local = None
        try:
            from backends.storage import is_storage_configured, get_storage
            import tempfile
            if is_storage_configured() and (clip.start_frame or clip.end_frame):
                storage = get_storage()
                if clip.start_frame and storage.exists(clip.start_frame):
                    _start_local = tempfile.mktemp(suffix='.png')
                    storage.download_file(clip.start_frame, _start_local)
                if clip.end_frame and clip.end_frame != clip.start_frame and storage.exists(clip.end_frame):
                    _end_local = tempfile.mktemp(suffix='.png')
                    storage.download_file(clip.end_frame, _end_local)
        except Exception as _dl_err:
            print(f"[Redo] Frame download for prompt regen failed: {_dl_err}")
        
        # Generate transition cue if start ≠ end
        _transition_cue = None
        if _start_local and _end_local and openai_key and video_config.use_openai_prompt_tuning:
            try:
                _duration = float(config_data.get('duration', '8'))
                _transition_cue = generate_transition_cue(
                    _start_local, _end_local,
                    clip.dialogue_text, language, _duration, openai_key)
            except Exception:
                pass
        
        # Build the new prompt with feedback integrated
        redo_feedback = request.reason if request and request.reason else None
        new_prompt = build_prompt(
            dialogue_line=clip.dialogue_text,
            start_frame_path=Path(_start_local) if _start_local else None,
            end_frame_path=Path(_end_local) if _end_local else None,
            clip_index=clip.clip_index,
            language=language,
            voice_profile=voice_profile,
            config=video_config,
            openai_key=openai_key,
            frame_analysis=frame_analysis,
            user_context_override=user_context_enriched,
            redo_feedback=redo_feedback,
            use_gesture_enrichment=config_data.get('use_gesture_enrichment', False),
            transition_cue=_transition_cue,
            short_dialogue_mode=config_data.get('short_dialogue_mode', 'optimized'),
            # v539 — prefix-short-lines settings
            prefix_short_enabled=config_data.get('prefix_short_enabled', False),
            prefix_short_word=config_data.get('prefix_short_word', 'only'),
            prefix_short_threshold=config_data.get('prefix_short_threshold', 15),
        )
        
        clip.prompt_text = new_prompt
        print(f"[Redo] ✓ Prompt regenerated for clip {clip.clip_index} ({len(new_prompt)} chars)")
        
        # Cleanup temp frames
        for _tf in [_start_local, _end_local]:
            if _tf:
                try: os.remove(_tf)
                except Exception: pass
        
    except Exception as regen_err:
        print(f"[Redo] ⚠ Prompt regeneration failed, keeping original: {regen_err}")
        import traceback; traceback.print_exc()
        # If regen fails, fall back to prepending feedback to existing prompt
        if request and request.reason and clip.prompt_text:
            clip.prompt_text = f"=== PRIORITY ===\n{request.reason}\n===\n\n{clip.prompt_text}"
    
    # Part C: Add debug log to prove DB state at redo time
    add_job_log(
        db, job.id,
        f"[RedoDebug] clip={clip.id} backend={job.backend} flow_url={'yes' if job.flow_project_url else 'no'} -> status={clip.status}",
        "DEBUG", "redo"
    )
    
    db.commit()
    
    add_job_log(
        db, clip.job_id, 
        f"Clip {clip.clip_index + 1} redo requested (attempt {new_attempt}/3, {'with' if clip.use_logged_params else 'without'} logged params, backend={job.backend})",
        "INFO", "approval",
        details={"reason": request.reason if request else None, "use_logged_params": clip.use_logged_params, "backend": job.backend}
    )
    
    return ApprovalResponse(
        clip_id=clip.id,
        status="redo_queued",  # UI always sees "redo_queued" for display purposes
        message=f"Redo queued (attempt {new_attempt}/3). {'Using same parameters.' if clip.use_logged_params else 'Using fresh parameters.'}",
        attempts_remaining=3 - new_attempt
    )


@app.post("/api/jobs/{job_id}/retry-stuck")
async def retry_stuck_clips(
    job_id: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v653 — bulk-retry stuck clips on a job.

    User report 2026-05-07 (job d09df1c): job had 25 lines, ~14 stuck
    in PENDING with "Waiting…" placeholder hours after submission.
    Worker had moved on / lost them. Pre-v653 the only remediation was
    per-clip "Redo" — but `request_clip_redo` rejects clips in PENDING
    status, so the user couldn't even queue the stuck ones.

    This endpoint scans the job for clips that are stuck and re-queues
    each via the same flow_redo_queued / redo_queued path the worker
    polls. Stuck = clips whose status is one of:
      - PENDING (never started)
      - GENERATING with claimed_at older than 10 minutes (worker died
        mid-job; claim went stale)
      - REDO_QUEUED / FLOW_REDO_QUEUED with updated_at older than
        10 minutes (worker dropped the redo)

    For each stuck clip:
      - Clear claim fields
      - Reset error_code / error_message / output_filename
      - Set status to flow_redo_queued (Flow backend) or redo_queued
        (API backend) — same status the existing worker polls
      - DO NOT bump generation_attempt — this is a worker-side retry,
        not a user-rejected redo. The 3-attempt limit still applies on
        actual user-initiated redos.

    Bumps job.updated_at so the worker's 24h activity window includes
    these reset clips on its next poll.
    """
    job = get_user_job(db, job_id, current_user)
    is_flow = job.backend == 'flow'
    target_status = ClipStatus.FLOW_REDO_QUEUED.value if is_flow else ClipStatus.REDO_QUEUED.value

    now = datetime.utcnow()
    stale_cutoff = now - timedelta(minutes=10)

    candidates = db.query(Clip).filter(Clip.job_id == job_id).all()
    reset_pending = []
    reset_stale_generating = []
    reset_stale_redo = []

    for clip in candidates:
        if clip.status == ClipStatus.PENDING.value:
            reset_pending.append(clip)
        elif clip.status == ClipStatus.GENERATING.value:
            claimed_at = getattr(clip, 'claimed_at', None)
            if claimed_at is None or claimed_at < stale_cutoff:
                reset_stale_generating.append(clip)
        elif clip.status in (ClipStatus.REDO_QUEUED.value, ClipStatus.FLOW_REDO_QUEUED.value):
            updated_at = getattr(clip, 'updated_at', None) or getattr(clip, 'created_at', None)
            if updated_at is None or updated_at < stale_cutoff:
                reset_stale_redo.append(clip)

    all_to_reset = reset_pending + reset_stale_generating + reset_stale_redo
    if not all_to_reset:
        return {
            "job_id": job_id,
            "reset_count": 0,
            "pending": 0,
            "stale_generating": 0,
            "stale_redo": 0,
            "message": "No stuck clips found.",
        }

    for clip in all_to_reset:
        clip.status = target_status
        clip.claimed_by_worker = None
        clip.claimed_at = None
        clip.error_code = None
        clip.error_message = None
        # Don't wipe output_filename if it exists (might be partial); the
        # worker will overwrite on success. For pure-pending clips it's
        # already None.

    job.updated_at = now
    if job.status in (JobStatus.PAUSED.value, JobStatus.FAILED.value):
        job.status = JobStatus.RUNNING.value

    add_job_log(
        db, job_id,
        f"Bulk retry-stuck: {len(all_to_reset)} clips re-queued "
        f"({len(reset_pending)} pending + {len(reset_stale_generating)} stale-generating "
        f"+ {len(reset_stale_redo)} stale-redo)",
        "INFO", "system",
    )
    db.commit()

    return {
        "job_id": job_id,
        "reset_count": len(all_to_reset),
        "pending": len(reset_pending),
        "stale_generating": len(reset_stale_generating),
        "stale_redo": len(reset_stale_redo),
        "message": f"Re-queued {len(all_to_reset)} stuck clip(s).",
    }


@app.get("/api/clips/{clip_id}")
async def get_clip(
    clip_id: int, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Get a single clip's data"""
    clip = get_user_clip(db, clip_id, current_user)
    
    return {
        "id": clip.id,
        "clip_index": clip.clip_index,
        "dialogue_id": clip.dialogue_id,
        "dialogue_text": clip.dialogue_text or "",
        "status": clip.status,
        "approval_status": clip.approval_status,
        "generation_attempt": clip.generation_attempt,
        "attempts_remaining": 3 - clip.generation_attempt,
    }


@app.get("/api/clips/{clip_id}/versions")
async def get_clip_versions(
    clip_id: int, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Get all generated versions of a clip"""
    clip = get_user_clip(db, clip_id, current_user)
    
    versions = json.loads(clip.versions_json) if clip.versions_json else []
    
    # Add current version if completed
    if clip.status == ClipStatus.COMPLETED.value and clip.output_filename:
        versions.append({
            "attempt": clip.generation_attempt,
            "filename": clip.output_filename,
            "generated_at": clip.completed_at.isoformat() if clip.completed_at else None,
            "approved": clip.approval_status == "approved",
            "start_frame": clip.start_frame,
            "end_frame": clip.end_frame,
            "current": True,
        })
    
    return {
        "clip_id": clip_id,
        "dialogue_id": clip.dialogue_id,
        "total_attempts": clip.generation_attempt,
        "attempts_remaining": 3 - clip.generation_attempt,
        "versions": versions,
    }


@app.post("/api/jobs/{job_id}/cleanup-versions")
async def cleanup_clip_versions(
    job_id: str, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Clean up duplicate versions in all clips of a job.
    Call this to fix clips that have duplicate entries in versions_json.
    """
    job = get_user_job(db, job_id, current_user)
    
    clips = db.query(Clip).filter(Clip.job_id == job_id).all()
    cleaned_count = 0
    
    for clip in clips:
        if not clip.versions_json:
            continue
            
        versions = json.loads(clip.versions_json)
        original_count = len(versions)
        
        # Deduplicate by version_key (attempt.variant) - keeps 1.1 and 1.2 separate
        seen = {}
        for v in versions:
            # Build version key from attempt.variant
            attempt = v.get("attempt", 1)
            variant = v.get("variant", 1)
            version_key = v.get("version_key") or f"{attempt}.{variant}"
            
            # Also dedupe by filename as backup (in case version_key is missing)
            filename = v.get("filename", "")
            key = version_key if version_key else filename
            
            if key:
                seen[key] = v
        
        cleaned_versions = sorted(seen.values(), key=lambda x: (x.get("attempt", 1), x.get("variant", 1)))
        
        if len(cleaned_versions) < original_count:
            clip.versions_json = json.dumps(cleaned_versions)
            cleaned_count += 1
            print(f"[Cleanup] Clip {clip.clip_index}: {original_count} -> {len(cleaned_versions)} versions", flush=True)
    
    db.commit()
    
    add_job_log(db, job_id, f"Cleaned up versions for {cleaned_count} clips", "INFO", "cleanup")
    
    return {
        "success": True,
        "clips_cleaned": cleaned_count,
        "total_clips": len(clips)
    }


@app.get("/api/jobs/{job_id}/review-status")
async def get_job_review_status(
    job_id: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Get summary of clip approval statuses for a job.

    v650: when the user has set a custom lineup (clip_order_json),
    EXCLUDE clips that are NOT in the lineup from the gating counts.
    Removed clips kept their pre-removal status (often pending_review),
    which made can_export stay false even when every clip the user
    actually wants is approved. Symptom (2026-05-07 owner report): 8/9
    approved, 1 removed from lineup but still pending → can_export=false,
    Export Final button stays disabled.

    Removed clips are still counted in `total_excluded_from_lineup` so
    the UI can surface them if needed, but they no longer block export.
    """
    job = get_user_job(db, job_id, current_user)

    clips = db.query(Clip).filter(Clip.job_id == job_id).all()

    # v650 — lineup-aware filtering. When a custom order is set, only
    # clips whose ids appear in clip_order_json count toward the
    # gating logic. When no override is set, all clips count (legacy).
    lineup_set = None
    if getattr(job, 'clip_order_json', None):
        try:
            import json as _json_lin
            lineup_set = set(_json_lin.loads(job.clip_order_json))
        except Exception:
            lineup_set = None

    in_lineup_clips = (
        [c for c in clips if c.id in lineup_set]
        if lineup_set is not None
        else clips
    )
    excluded_count = len(clips) - len(in_lineup_clips) if lineup_set is not None else 0

    summary = {
        "total": len(in_lineup_clips),
        "total_all": len(clips),  # v650 — for UI: all clips regardless of lineup
        "total_excluded_from_lineup": excluded_count,  # v650
        "pending_review": 0,
        "approved": 0,
        "redo_queued": 0,
        "max_attempts": 0,
        "generating": 0,
        "failed": 0,
        "skipped": 0,
    }

    for c in in_lineup_clips:
        if c.status == ClipStatus.COMPLETED.value:
            if c.approval_status == "approved":
                summary["approved"] += 1
            elif c.approval_status == "max_attempts":
                summary["max_attempts"] += 1
            else:
                summary["pending_review"] += 1
        elif c.status in [ClipStatus.REDO_QUEUED.value, ClipStatus.FLOW_REDO_QUEUED.value]:
            summary["redo_queued"] += 1
        elif c.status in [ClipStatus.GENERATING.value, ClipStatus.PENDING.value]:
            summary["generating"] += 1
        elif c.status == ClipStatus.FAILED.value:
            summary["failed"] += 1
        elif c.status == ClipStatus.SKIPPED.value:
            summary["skipped"] += 1

    summary["all_approved"] = summary["approved"] > 0 and summary["approved"] + summary["skipped"] == summary["total"]
    # Can export if we have approved clips and nothing is still processing
    summary["can_export"] = summary["approved"] > 0 and summary["generating"] == 0 and summary["redo_queued"] == 0 and summary["pending_review"] == 0
    # Can open lineup as soon as any clip is approved (even while others generate)
    summary["can_lineup"] = summary["approved"] > 0
    summary["needs_attention"] = summary["max_attempts"] > 0 or summary["failed"] > 0
    summary["has_lineup_override"] = getattr(job, 'clip_order_json', None) is not None

    return summary


# ============ Logs ============

@app.get("/api/jobs/{job_id}/logs", response_model=List[LogResponse])
async def get_job_logs(
    job_id: str,
    since_id: int = 0,
    limit: int = Query(default=100, le=500),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Get logs for a job (supports polling with since_id)"""
    job = get_user_job(db, job_id, current_user)
    
    logs = get_job_logs_since(db, job_id, since_id)[:limit]
    
    return [
        LogResponse(
            id=log.id,
            created_at=log.created_at.isoformat() if log.created_at else "",
            level=log.level,
            category=log.category,
            clip_index=log.clip_index,
            message=log.message,
        )
        for log in logs
    ]


@app.get("/api/jobs/{job_id}/backup-status")
async def get_job_backup_status(
    job_id: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Check if a job's frames are backed up to cloud storage.
    
    This is important for redo functionality - if frames are not backed up,
    redo will fail if the local files are deleted.
    """
    job = get_user_job(db, job_id, current_user)
    
    # Check local files
    images_dir = Path(job.images_dir) if job.images_dir else None
    local_files_exist = False
    local_file_count = 0
    
    if images_dir and images_dir.exists():
        try:
            image_extensions = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
            local_files = [f for f in images_dir.iterdir() if f.suffix.lower() in image_extensions]
            local_file_count = len(local_files)
            local_files_exist = local_file_count > 0
        except Exception as e:
            local_files_exist = False
    
    # Check cloud backup
    cloud_backup_exists = False
    cloud_file_count = 0
    
    if job.frames_storage_keys:
        try:
            keys = json.loads(job.frames_storage_keys)
            cloud_file_count = len(keys)
            cloud_backup_exists = cloud_file_count > 0
        except:
            pass
    
    # Determine redo capability
    can_redo = local_files_exist or cloud_backup_exists
    
    return {
        "job_id": job_id,
        "local_files": {
            "exist": local_files_exist,
            "count": local_file_count,
            "path": str(images_dir) if images_dir else None,
        },
        "cloud_backup": {
            "exist": cloud_backup_exists,
            "count": cloud_file_count,
        },
        "can_redo": can_redo,
        "redo_source": "local" if local_files_exist else ("cloud" if cloud_backup_exists else "none"),
        "message": (
            "✓ Redo available (local files exist)" if local_files_exist else
            "✓ Redo available (cloud backup exists)" if cloud_backup_exists else
            "⚠️ Redo NOT available - local files deleted and no cloud backup. Create a new job with re-uploaded images."
        )
    }


# ============ Server-Sent Events ============

@app.get("/api/jobs/{job_id}/stream")
async def stream_job_events(
    job_id: str, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Stream job events via Server-Sent Events.
    
    Events:
    - progress: Clip progress update
    - clip_started: Clip generation started
    - clip_completed: Clip generation completed
    - error: Error occurred
    - job_completed: Job finished
    """
    job = get_user_job(db, job_id, current_user)
    
    async def event_generator():
        event_queue = worker.subscribe(job_id)
        
        try:
            # Send initial status
            yield f"data: {json.dumps({'type': 'status', 'status': job.status, 'progress': job.progress_percent})}\n\n"
            
            while True:
                try:
                    # Non-blocking check
                    event = event_queue.get(timeout=30)
                    yield f"data: {json.dumps(event)}\n\n"
                    
                    # Stop streaming if job completed
                    if event.get("type") == "job_completed":
                        break
                        
                except Exception:
                    # Send keepalive
                    yield f": keepalive\n\n"
                    
                    # Check if job is still active
                    from models import get_db
                    with get_db() as check_db:
                        check_job = check_db.query(Job).filter(Job.id == job_id).first()
                        if check_job and check_job.status in [
                            JobStatus.COMPLETED.value,
                            JobStatus.FAILED.value,
                            JobStatus.CANCELLED.value,
                        ]:
                            break
        finally:
            worker.unsubscribe(job_id, event_queue)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


# ============ Downloads ============

@app.get("/api/jobs/{job_id}/outputs")
async def list_outputs(
    job_id: str, 
    approved_only: bool = False,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    List generated videos for a job.
    
    If approved_only=True, only returns videos from approved clips (selected variants).
    Falls back to filesystem listing if job not in database (e.g., after server restart).
    Supports both local filesystem and R2 storage.
    """
    job = db.query(Job).filter(Job.id == job_id).first()
    
    # Try to find output directory even without database entry
    if job:
        output_dir = Path(job.output_dir)
    else:
        # Fallback: check if directory exists directly
        output_dir = app_config.outputs_dir / job_id
    
    videos = []
    
    if approved_only and job:
        # Only return approved clips' selected variants (requires DB)
        clips = db.query(Clip).filter(
            Clip.job_id == job_id,
            Clip.approval_status == "approved"
        ).order_by(Clip.clip_index).all()
        
        for clip in clips:
            if clip.output_filename:
                # Check local filesystem first
                filepath = output_dir / clip.output_filename if output_dir.exists() else None
                if filepath and filepath.exists():
                    videos.append({
                        "filename": clip.output_filename,
                        "size": filepath.stat().st_size,
                        "url": f"/api/jobs/{job_id}/outputs/{clip.output_filename}",
                        "clip_index": clip.clip_index,
                        "variant": clip.selected_variant,
                    })
                elif clip.output_url:
                    # R2 storage - use the API endpoint which proxies from R2
                    videos.append({
                        "filename": clip.output_filename,
                        "size": 0,  # Unknown size for R2 files
                        "url": f"/api/jobs/{job_id}/outputs/{clip.output_filename}",
                        "clip_index": clip.clip_index,
                        "variant": clip.selected_variant,
                        "storage": "r2"
                    })
                else:
                    # Local file missing and no output_url - try R2 by filename as fallback
                    try:
                        from backends.storage import is_storage_configured, get_storage
                        if is_storage_configured():
                            storage = get_storage()
                            r2_key = f"jobs/{job_id}/outputs/{clip.output_filename}"
                            if storage.exists(r2_key):
                                videos.append({
                                    "filename": clip.output_filename,
                                    "size": 0,
                                    "url": f"/api/jobs/{job_id}/outputs/{clip.output_filename}",
                                    "clip_index": clip.clip_index,
                                    "variant": clip.selected_variant,
                                    "storage": "r2"
                                })
                    except Exception as e:
                        print(f"[Outputs] R2 check error for clip {clip.clip_index}: {e}", flush=True)
    else:
        # Return all videos from filesystem
        if output_dir.exists():
            for f in output_dir.glob("*.mp4"):
                # Try to extract clip index from filename (e.g., "1_image_00_..." -> clip 1)
                clip_idx = None
                try:
                    parts = f.stem.split("_")
                    if parts[0].isdigit():
                        clip_idx = int(parts[0])
                except:
                    pass
                
                videos.append({
                    "filename": f.name,
                    "size": f.stat().st_size,
                    "url": f"/api/jobs/{job_id}/outputs/{f.name}",
                    "clip_index": clip_idx,
                })
        
        # Also check R2 for job outputs (all backend types)
        try:
            from backends.storage import is_storage_configured, get_storage
            
            if is_storage_configured():
                storage = get_storage()
                r2_prefix = f"jobs/{job_id}/outputs/"
                r2_keys = storage.list_objects(prefix=r2_prefix, max_keys=100)
                
                existing_filenames = {v["filename"] for v in videos}
                
                for key in r2_keys:
                    filename = key.split("/")[-1]
                    if filename and filename.endswith(".mp4") and filename not in existing_filenames:
                        # Extract clip index from filename (clip_0.mp4 -> 0)
                        clip_idx = None
                        try:
                            if filename.startswith("clip_"):
                                clip_idx = int(filename.replace("clip_", "").replace(".mp4", ""))
                        except:
                            pass
                        
                        videos.append({
                            "filename": filename,
                            "size": 0,
                            "url": f"/api/jobs/{job_id}/outputs/{filename}",
                            "clip_index": clip_idx,
                            "storage": "r2"
                        })
        except Exception as e:
            print(f"[Outputs] R2 list error: {e}", flush=True)
    
    videos.sort(key=lambda x: x.get("clip_index") or 0 if approved_only else x["filename"])
    
    return {"job_id": job_id, "videos": videos, "count": len(videos)}


@app.get("/api/jobs/{job_id}/outputs/{filename}")
async def download_output(
    job_id: str,
    filename: str,
    request: Request,
):
    """Download a generated video. Works with local filesystem or R2 storage."""
    # Use a short-lived DB context instead of dependency injection so we can
    # release the connection immediately — the R2 download can take 30s+
    # and holding a dependency-injected session that long exhausts the pool.
    from models import get_db
    from auth import validate_session as db_validate_session

    output_dir = None
    with get_db() as _db:
        # Authenticate
        if GOOGLE_AUTH_ENABLED:
            session_token = request.cookies.get("session")
            if not session_token:
                raise HTTPException(status_code=401, detail="Not authenticated")
            user = db_validate_session(_db, session_token)
            if not user or not user.is_active:
                raise HTTPException(status_code=401, detail="Not authenticated")
            job = _db.query(Job).filter(Job.id == job_id, Job.user_id == user.id).first()
        else:
            job = _db.query(Job).filter(Job.id == job_id).first()

        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        output_dir = Path(job.output_dir)
    # DB connection released here — before any I/O

    filepath = output_dir / filename

    # Method 1: Local filesystem (fast path)
    # Filenames are unique per version (UUID suffix) — never mutate, safe
    # to cache forever. v638: bumped from 86400 (24h) → 31536000 (365d)
    # so the browser never evicts videos that have been viewed once.
    # User reported clips reload on every job open; 24h cache lost on
    # day-overs / cache pressure / private mode quirks. 365d + immutable
    # = browser disk cache hit on every subsequent open.
    video_cache_headers = {"Cache-Control": "private, max-age=31536000, immutable"}
    if filepath.exists():
        return FileResponse(filepath, media_type="video/mp4", filename=filename, headers=video_cache_headers)

    # Method 2: R2 storage — cache to disk on first request, stream on subsequent ones
    # Uses .download.tmp staging to prevent race condition (partial file served).
    try:
        from backends.storage import is_storage_configured, get_storage

        if is_storage_configured():
            storage = get_storage()
            r2_key = f"jobs/{job_id}/outputs/{filename}"

            if storage.exists(r2_key):
                output_dir.mkdir(parents=True, exist_ok=True)
                tmp_filepath = filepath.with_suffix(".download.tmp")

                # Wait if another request is already downloading this file
                waited = 0
                while tmp_filepath.exists() and waited < 30:
                    await asyncio.sleep(0.5)
                    waited += 0.5
                    if filepath.exists():
                        break

                if not filepath.exists():
                    await asyncio.to_thread(storage.download_file, r2_key, str(tmp_filepath))
                    tmp_filepath.rename(filepath)
                    print(f"[Download] Cached from R2: {filename}", flush=True)

                return FileResponse(filepath, media_type="video/mp4", filename=filename, headers=video_cache_headers)
    except Exception as e:
        print(f"[Download] R2 error: {e}", flush=True)

    raise HTTPException(status_code=404, detail="File not found")


@app.get("/api/jobs/{job_id}/missing-clips")
async def download_missing_clips(
    job_id: str, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Download the missing clips Excel file for celebrity-filtered clips."""
    job = get_user_job(db, job_id, current_user)
    
    output_dir = Path(job.output_dir)
    
    # Try xlsx first, then csv, then json (fallback)
    for ext, media_type in [
        ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        ("csv", "text/csv"),
        ("json", "application/json")
    ]:
        filepath = output_dir / f"missing_clips.{ext}"
        if filepath.exists():
            return FileResponse(
                filepath,
                media_type=media_type,
                filename=f"missing_clips.{ext}",
            )
    
    raise HTTPException(status_code=404, detail="Missing clips file not found")


# ============ Prompt Viewer ============

@app.get("/api/jobs/{job_id}/prompts")
async def get_job_prompts(
    job_id: str,
    request: Request,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Get all prompts for a job with frame references.

    v678c: also expose the per-clip veo_prompt_override + negative override
    + dialogue_pad parsed from job.dialogue_json. When clip.prompt_text is
    empty (e.g. background prompt-build hasn't run yet, or the clip row
    was created before v572 stamped it), the frontend can fall back to
    the prebuilt override and still render meaningful content. Also adds
    char-count fields so the UI can render compact badges instead of
    measuring inside the browser.
    """
    job = get_user_job(db, job_id, current_user)

    clips = db.query(Clip).filter(
        Clip.job_id == job_id
    ).order_by(Clip.clip_index).all()

    base_url = str(request.base_url).rstrip('/')

    # v678c — pull the lines list from dialogue_json so we can index
    # per-clip overrides + pads. Tolerates legacy plain-list shape.
    overrides_by_idx: Dict[int, Dict[str, Any]] = {}
    pads_by_idx: Dict[int, Optional[str]] = {}
    try:
        dlg_raw = json.loads(job.dialogue_json) if job.dialogue_json else {}
        if isinstance(dlg_raw, dict):
            lines_list = dlg_raw.get("lines") or []
        elif isinstance(dlg_raw, list):
            lines_list = dlg_raw
        else:
            lines_list = []
        for idx, l in enumerate(lines_list):
            if isinstance(l, dict):
                tp = (l.get("veo_prompt_override") or "").strip() or None
                np = (l.get("veo_negative_prompt_override") or "").strip() or None
                if tp or np:
                    overrides_by_idx[idx] = {"text_prompt": tp, "negative_prompt": np}
                pad = (l.get("dialogue_pad") or "").strip() or None
                if pad:
                    pads_by_idx[idx] = pad
    except Exception as e:
        print(f"[prompts] dialogue_json parse warning: {e}", flush=True)

    prompts = []
    for clip in clips:
        start_filename = clip.start_frame.split('/')[-1] if clip.start_frame else None
        end_filename = clip.end_frame.split('/')[-1] if clip.end_frame else None

        # Effective prompt text: prefer the stamped clip.prompt_text (the
        # final composed prompt build_prompt returned), fall back to the
        # raw veo_prompt_override (with negative trailer joined the same
        # way build_prompt does it).
        text = clip.prompt_text or ""
        ovr = overrides_by_idx.get(clip.clip_index)
        if not text and ovr:
            tp = ovr.get("text_prompt") or ""
            np = ovr.get("negative_prompt") or ""
            text = tp + (f"\n\nNegative prompt: {np}" if np else "")

        prompts.append({
            "clip_index": clip.clip_index,
            "dialogue_text": clip.dialogue_text,
            "prompt_text": text,
            "prompt_text_chars": len(text),
            "is_prebuilt": clip.clip_index in overrides_by_idx,
            "veo_prompt_override": ovr.get("text_prompt") if ovr else None,
            "veo_negative_prompt_override": ovr.get("negative_prompt") if ovr else None,
            "dialogue_pad": pads_by_idx.get(clip.clip_index),
            "start_frame": clip.start_frame,
            "end_frame": clip.end_frame,
            "start_frame_url": f"{base_url}/api/jobs/{job_id}/images/{start_filename}" if start_filename else None,
            "end_frame_url": f"{base_url}/api/jobs/{job_id}/images/{end_filename}" if end_filename else None,
            "clip_mode": clip.clip_mode,
            "scene_index": clip.scene_index,
        })

    return {
        "job_id": job_id,
        "backend": job.backend,
        "total_clips": len(prompts),
        "prompts": prompts,
    }


@app.get("/api/jobs/{job_id}/prompts/download")
async def download_job_prompts(
    job_id: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Download all prompts as a .txt file."""
    from fastapi.responses import Response
    
    job = get_user_job(db, job_id, current_user)
    
    clips = db.query(Clip).filter(
        Clip.job_id == job_id
    ).order_by(Clip.clip_index).all()
    
    lines = []
    for clip in clips:
        lines.append(f"{'=' * 60}")
        lines.append(f"CLIP {clip.clip_index + 1}")
        lines.append(f"{'=' * 60}")
        if clip.start_frame:
            lines.append(f"Start frame: {clip.start_frame.split('/')[-1]}")
        if clip.end_frame:
            lines.append(f"End frame: {clip.end_frame.split('/')[-1]}")
        lines.append("")
        lines.append(clip.prompt_text or "(no prompt)")
        lines.append("")
        lines.append("")
    
    content = "\n".join(lines)
    
    return Response(
        content=content,
        media_type="text/plain",
        headers={
            "Content-Disposition": f'attachment; filename="prompts_{job_id[:8]}.txt"'
        }
    )


# ============ Assembly Mode ============

@app.post("/api/jobs/assemble")
async def assemble_job(
    script: str = Form(""),
    clips: List[UploadFile] = File(...),
    order_by: str = Form("match"),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Create an assembly job from uploaded clips + script.
    Phase 1 (instant): save files, create job as 'preparing', return ID.
    Phase 2 (background): Whisper transcription, fuzzy matching, R2 upload, clip creation.
    """
    import tempfile
    
    # Validate
    script_lines = [l.strip() for l in script.strip().split('\n') if l.strip()] if script.strip() else []
    if not script_lines and order_by == "match":
        raise HTTPException(status_code=400, detail="Script is required for Auto-match mode")
    if len(clips) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 clips allowed")
    if len(clips) == 0:
        raise HTTPException(status_code=400, detail="No clips uploaded")
    
    if not script_lines:
        script_lines = [f"Clip {i+1}" for i in range(len(clips))]
    
    job_id = str(uuid.uuid4())
    output_dir = app_config.outputs_dir / job_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"[Assemble] Creating job {job_id[:8]} with {len(clips)} clips and {len(script_lines)} script lines (order_by={order_by})", flush=True)
    
    # Phase 1: Save clip files to temp (fast — just reading upload buffers)
    clip_files = []  # [{temp_path, original_filename, index, size}]
    for i, clip_file in enumerate(clips):
        content = await clip_file.read()
        if len(content) == 0:
            continue
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4', dir=str(output_dir))
        tmp.write(content)
        tmp.close()
        clip_files.append({
            'temp_path': tmp.name,
            'original_filename': clip_file.filename or f"clip_{i}.mp4",
            'index': i,
            'size': len(content),
        })
    
    if not clip_files:
        raise HTTPException(status_code=400, detail="No valid clips uploaded")
    
    # Create job with 'preparing' status
    dialogue_list = [{"id": i + 1, "text": line} for i, line in enumerate(script_lines)]
    job = Job(
        id=job_id,
        user_id=current_user.id,
        status="preparing",
        config_json=json.dumps({"language": "English", "duration": "8", "assembly_mode": True, "order_by": order_by}),
        dialogue_json=json.dumps({"lines": dialogue_list}),
        images_dir=str(output_dir),
        output_dir=str(output_dir),
        total_clips=len(script_lines),
        completed_clips=0,
        backend="import",
        progress_percent=0.0,
        started_at=datetime.utcnow(),
    )
    db.add(job)
    db.commit()
    
    add_job_log(db, job_id, f"Assembly job created — {len(clip_files)} clips uploaded, processing in background...")
    db.commit()
    
    # Phase 2: Launch background task
    import asyncio
    asyncio.create_task(_assemble_job_background(job_id, clip_files, script_lines, order_by, current_user.id))
    
    return {"id": job_id, "status": "preparing", "total_clips": len(script_lines)}
    
async def _assemble_job_background(job_id: str, clip_files: list, script_lines: list, order_by: str, user_id: str):
    """Background task: transcribe, match, upload to R2, create clips."""
    import subprocess, difflib, re as _re_bg
    
    db = next(get_db_session())
    try:
        add_job_log(db, job_id, f"Processing {len(clip_files)} clips (order_by={order_by})...")
        db.commit()
        
        # Storage setup
        from backends.storage import is_storage_configured, get_storage
        storage = None
        if is_storage_configured():
            try:
                storage = get_storage()
            except Exception as e:
                print(f"[Assemble] Storage warning: {e}", flush=True)
        
        clip_data = clip_files  # Already saved to temp in phase 1
        
        # === Ordering strategy ===
        if order_by == "filename":
            print(f"[Assemble] Ordering by filename (skipping transcription)", flush=True)
            add_job_log(db, job_id, "Ordering clips by filename...")
            db.commit()
            
            sorted_clips = sorted(clip_data, key=lambda cd: cd['original_filename'].lower())
            ordered_clips = []
            match_scores = {}
            
            for i, cd in enumerate(sorted_clips):
                if i < len(script_lines):
                    ordered_clips.append({'clip_data': cd, 'line_idx': i, 'dialogue': script_lines[i], 'transcript': ''})
                    match_scores[i] = 1.0
                else:
                    ordered_clips.append({'clip_data': cd, 'line_idx': len(script_lines) + (i - len(script_lines)), 'dialogue': cd['original_filename'], 'transcript': ''})
            
            matched_count = min(len(sorted_clips), len(script_lines))
            total = len(script_lines)
            print(f"[Assemble] Paired {matched_count}/{total} clips by filename order", flush=True)
        
        else:
            # Auto-match: transcribe + fuzzy-match
            print(f"[Assemble] Transcribing {len(clip_data)} clips with Whisper...", flush=True)
            add_job_log(db, job_id, f"Transcribing {len(clip_data)} clips with Whisper...")
            db.commit()
            
            transcriptions = []
            whisper_model = None
            try:
                from faster_whisper import WhisperModel
                whisper_model = await asyncio.to_thread(WhisperModel, "base", device="cpu", compute_type="int8")
                print(f"[Assemble] Whisper model loaded (base, cpu, int8)", flush=True)
            except Exception as e:
                print(f"[Assemble] Whisper load error: {e}", flush=True)
            
            for cd in clip_data:
                transcript_text = ""
                if whisper_model:
                    audio_path = cd['temp_path'] + '.wav'
                    try:
                        await asyncio.to_thread(subprocess.run, [
                            'ffmpeg', '-y', '-i', cd['temp_path'],
                            '-vn', '-acodec', 'pcm_s16le', '-ar', '16000', '-ac', '1',
                            audio_path
                        ], capture_output=True, timeout=30)
                        
                        if os.path.exists(audio_path) and os.path.getsize(audio_path) > 1000:
                            def _transcribe():
                                segs, _info = whisper_model.transcribe(audio_path, language="en")
                                return " ".join([s.text.strip() for s in segs]).strip()
                            transcript_text = await asyncio.to_thread(_transcribe)
                            print(f"[Assemble] Clip {cd['index']}: '{transcript_text[:80]}...'", flush=True)
                        
                        if os.path.exists(audio_path):
                            os.remove(audio_path)
                    except Exception as e:
                        print(f"[Assemble] Transcription error clip {cd['index']}: {e}", flush=True)
                
                transcriptions.append({'index': cd['index'], 'text': transcript_text, 'original_filename': cd['original_filename']})
            
            if whisper_model:
                del whisper_model
                import gc; gc.collect()
            
            add_job_log(db, job_id, f"Matching {len(transcriptions)} clips to {len(script_lines)} script lines...")
            db.commit()
            
            # Fuzzy matching
            def normalize_text(text):
                text = text.lower().strip()
                text = _re_bg.sub(r'[^\w\s]', '', text)
                text = _re_bg.sub(r'\s+', ' ', text)
                return text
            
            def first_n_words(text, n=6):
                return " ".join(normalize_text(text).split()[:n])
            
            MATCH_WORDS = 6
            scores = []
            for t in transcriptions:
                if not t['text']:
                    for li in range(len(script_lines)):
                        scores.append((0.0, t['index'], li))
                    continue
                clip_start = first_n_words(t['text'], MATCH_WORDS)
                for li, line in enumerate(script_lines):
                    line_start = first_n_words(line, MATCH_WORDS)
                    start_score = difflib.SequenceMatcher(None, clip_start, line_start).ratio()
                    full_score = difflib.SequenceMatcher(None, normalize_text(t['text']), normalize_text(line)).ratio()
                    combined = start_score * 0.8 + full_score * 0.2
                    scores.append((combined, t['index'], li))
            
            scores.sort(reverse=True)
            assigned_clips = {}
            assigned_lines = {}
            match_scores = {}
            
            for score, clip_idx, line_idx in scores:
                if clip_idx in assigned_lines or line_idx in assigned_clips:
                    continue
                assigned_clips[line_idx] = clip_idx
                assigned_lines[clip_idx] = line_idx
                match_scores[line_idx] = score
            
            unmatched_clips = [cd['index'] for cd in clip_data if cd['index'] not in assigned_lines]
            next_line = len(script_lines)
            for ci in sorted(unmatched_clips):
                assigned_lines[ci] = next_line
                next_line += 1
            
            # Log matching results
            for line_idx in range(len(script_lines)):
                if line_idx in assigned_clips:
                    ci = assigned_clips[line_idx]
                    score = match_scores.get(line_idx, 0)
                    icon = "✅" if score > 0.5 else "⚠️" if score > 0.3 else "❌"
                    clip_file = clip_data[ci]['original_filename'][:25]
                    script_text = script_lines[line_idx][:50]
                    print(f"[Assemble] {icon} #{line_idx+1} ({score:.2f}): {script_text} → {clip_file}", flush=True)
            
            matched_count = sum(1 for s in match_scores.values() if s > 0.5)
            total = len(script_lines)
            add_job_log(db, job_id, f"Matched {matched_count}/{total} clips ({matched_count/total*100:.0f}% strong)")
            db.commit()
            
            # Build ordered list
            ordered_clips = []
            for line_idx in range(len(script_lines)):
                if line_idx in assigned_clips:
                    ci = assigned_clips[line_idx]
                    cd = clip_data[ci]
                    ordered_clips.append({
                        'clip_data': cd, 'line_idx': line_idx,
                        'dialogue': script_lines[line_idx],
                        'transcript': next((t['text'] for t in transcriptions if t['index'] == ci), ''),
                    })
            for ci in sorted(unmatched_clips):
                cd = clip_data[ci]
                ordered_clips.append({
                    'clip_data': cd, 'line_idx': assigned_lines[ci],
                    'dialogue': cd['original_filename'],
                    'transcript': next((t['text'] for t in transcriptions if t['index'] == ci), ''),
                })
        
        # Upload clips to R2 and create DB records
        add_job_log(db, job_id, f"Uploading {len(ordered_clips)} clips to storage...")
        db.commit()
        
        for new_idx, oc in enumerate(ordered_clips):
            cd = oc['clip_data']
            new_filename = f"clip_{new_idx}_1.1.mp4"
            new_r2_key = f"jobs/{job_id}/outputs/{new_filename}"
            
            if storage and os.path.exists(cd['temp_path']):
                try:
                    await asyncio.to_thread(storage.upload_file, cd['temp_path'], new_r2_key, 'video/mp4')
                except Exception as e:
                    print(f"[Assemble] R2 upload error clip {new_idx}: {e}", flush=True)
            
            clip = Clip(
                job_id=job_id, clip_index=new_idx, dialogue_id=new_idx + 1,
                dialogue_text=oc['dialogue'], status=ClipStatus.COMPLETED.value,
                approval_status="approved", output_filename=new_filename,
                generation_attempt=1,
                versions_json=json.dumps([{
                    "attempt": 1, "variant": 1, "version_key": "1.1",
                    "filename": new_filename,
                    "generated_at": datetime.utcnow().isoformat(), "approved": True,
                }]),
                selected_variant=1, completed_at=datetime.utcnow(),
            )
            db.add(clip)
        
        # Update job to completed
        job = db.query(Job).filter(Job.id == job_id).first()
        if job:
            job.status = JobStatus.COMPLETED.value
            job.completed_clips = len(ordered_clips)
            job.progress_percent = 100.0
            job.completed_at = datetime.utcnow()
        
        db.commit()
        
        # Cleanup temp files
        for cd in clip_data:
            try:
                if os.path.exists(cd['temp_path']):
                    os.remove(cd['temp_path'])
            except Exception:
                pass
        
        add_job_log(db, job_id, f"✓ Assembly complete — {len(ordered_clips)} clips ready for export")
        db.commit()
        print(f"[Assemble] ✓ Job {job_id[:8]} completed with {len(ordered_clips)} clips", flush=True)
        
    except Exception as e:
        print(f"[Assemble] ❌ Background error for {job_id[:8]}: {e}", flush=True)
        import traceback; traceback.print_exc()
        try:
            job = db.query(Job).filter(Job.id == job_id).first()
            if job:
                job.status = "failed"
                job.error_message = str(e)[:500]
            add_job_log(db, job_id, f"❌ Assembly failed: {e}", "ERROR")
            db.commit()
        except Exception:
            pass
    finally:
        db.close()


@app.post("/api/jobs/{job_id}/attach-clips")
async def attach_clips_to_job(
    job_id: str,
    clips: List[UploadFile] = File(...),
    order_by: str = Form("match"),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Attach uploaded video clips to an existing prompt_only job.
    
    order_by: "filename" = sort clips alphabetically, pair 1:1 with dialogue lines
              "match" = transcribe clips, fuzzy-match to dialogue lines
    """
    import tempfile
    
    job = get_user_job(db, job_id, current_user)
    
    if job.backend not in ('prompt_only', 'flow', 'api'):
        raise HTTPException(status_code=400, detail="Attach clips is only supported for prompt/flow/api jobs")
    
    existing_clips = db.query(Clip).filter(Clip.job_id == job_id).order_by(Clip.clip_index).all()
    if not existing_clips:
        raise HTTPException(status_code=400, detail="Job has no clip slots to attach to")
    
    if len(clips) == 0:
        raise HTTPException(status_code=400, detail="No clips uploaded")
    
    from backends.storage import is_storage_configured, get_storage
    storage = None
    if is_storage_configured():
        try:
            storage = get_storage()
        except Exception as e:
            print(f"[AttachClips] Storage warning: {e}", flush=True)
    
    output_dir = Path(job.output_dir) if job.output_dir else (app_config.outputs_dir / job_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"[AttachClips] Job {job_id[:8]}: attaching {len(clips)} clips to {len(existing_clips)} slots (order_by={order_by})", flush=True)
    
    # Save uploaded clips to temp
    clip_data = []
    for i, clip_file in enumerate(clips):
        content = await clip_file.read()
        if len(content) == 0:
            continue
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4', dir=str(output_dir))
        tmp.write(content)
        tmp.close()
        clip_data.append({
            'temp_path': tmp.name,
            'original_filename': clip_file.filename or f"clip_{i}.mp4",
            'index': i,
            'size': len(content),
        })
    
    if not clip_data:
        raise HTTPException(status_code=400, detail="No valid clips uploaded")
    
    # Build ordered mapping: which uploaded clip → which existing clip slot
    ordered_pairs = []  # [{clip_data, db_clip}]
    
    if order_by == "filename":
        sorted_uploads = sorted(clip_data, key=lambda cd: cd['original_filename'].lower())
        for i, cd in enumerate(sorted_uploads):
            if i < len(existing_clips):
                ordered_pairs.append({'clip_data': cd, 'db_clip': existing_clips[i]})
        print(f"[AttachClips] Paired {len(ordered_pairs)} clips by filename order", flush=True)
    
    else:
        # Transcribe + fuzzy-match
        import subprocess
        import difflib
        
        transcriptions = []
        whisper_model = None
        try:
            from faster_whisper import WhisperModel
            whisper_model = WhisperModel("base", device="cpu", compute_type="int8")
        except Exception as e:
            print(f"[AttachClips] Whisper load error: {e}", flush=True)
        
        for cd in clip_data:
            transcript_text = ""
            if whisper_model:
                try:
                    audio_tmp = cd['temp_path'].replace('.mp4', '.wav')
                    subprocess.run(['ffmpeg', '-i', cd['temp_path'], '-ar', '16000', '-ac', '1',
                                    '-f', 'wav', audio_tmp, '-y'], capture_output=True, timeout=30)
                    if os.path.exists(audio_tmp):
                        segments, _ = whisper_model.transcribe(audio_tmp, language="en")
                        transcript_text = " ".join(s.text.strip() for s in segments).strip()
                        os.remove(audio_tmp)
                except Exception as e:
                    print(f"[AttachClips] Transcription error for {cd['original_filename']}: {e}", flush=True)
            transcriptions.append({'index': cd['index'], 'text': transcript_text, 'clip_data': cd})
            print(f"[AttachClips] Transcribed {cd['original_filename']}: {transcript_text[:60]}", flush=True)
        
        # Fuzzy match transcriptions to dialogue lines
        dialogue_texts = [c.dialogue_text or '' for c in existing_clips]
        used_clips = set()
        used_slots = set()
        matches = []
        
        for si, dial in enumerate(dialogue_texts):
            best_score = 0
            best_ci = None
            for t in transcriptions:
                if t['index'] in used_clips:
                    continue
                score = difflib.SequenceMatcher(None, dial.lower(), t['text'].lower()).ratio()
                if score > best_score:
                    best_score = score
                    best_ci = t['index']
            if best_ci is not None and best_score > 0.2:
                matches.append((si, best_ci, best_score))
                used_clips.add(best_ci)
                used_slots.add(si)
        
        for si, ci, score in matches:
            cd = next(t['clip_data'] for t in transcriptions if t['index'] == ci)
            ordered_pairs.append({'clip_data': cd, 'db_clip': existing_clips[si]})
            print(f"[AttachClips] Match: slot {si+1} ← {cd['original_filename']} (score={score:.2f})", flush=True)
        
        # Unmatched uploads → fill remaining slots in order
        remaining_slots = [c for i, c in enumerate(existing_clips) if i not in used_slots]
        remaining_uploads = [t['clip_data'] for t in transcriptions if t['index'] not in used_clips]
        for cd, db_clip in zip(remaining_uploads, remaining_slots):
            ordered_pairs.append({'clip_data': cd, 'db_clip': db_clip})
        
        print(f"[AttachClips] Matched {len(matches)}/{len(existing_clips)} by voiceover", flush=True)
    
    # Upload to R2 and update DB clips
    attached = 0
    for pair in ordered_pairs:
        cd = pair['clip_data']
        db_clip = pair['db_clip']
        ci = db_clip.clip_index
        
        new_filename = f"clip_{ci}_1.1.mp4"
        new_r2_key = f"jobs/{job_id}/outputs/{new_filename}"
        
        if storage and os.path.exists(cd['temp_path']):
            try:
                storage.upload_file(cd['temp_path'], new_r2_key, content_type='video/mp4')
            except Exception as e:
                print(f"[AttachClips] R2 upload error clip {ci}: {e}", flush=True)
                continue
        
        output_url = f"/api/jobs/{job_id}/outputs/{new_filename}"
        db_clip.output_filename = new_filename
        db_clip.output_url = output_url
        db_clip.status = ClipStatus.COMPLETED.value
        db_clip.approval_status = "pending_review"
        db_clip.generation_attempt = 1
        db_clip.selected_variant = 1
        db_clip.completed_at = datetime.utcnow()
        db_clip.versions_json = json.dumps([{
            "attempt": 1,
            "variant": 1,
            "version_key": "1.1",
            "filename": new_filename,
            "generated_at": datetime.utcnow().isoformat(),
        }])
        attached += 1
    
    # Update job progress
    completed = db.query(Clip).filter(
        Clip.job_id == job_id,
        Clip.status == ClipStatus.COMPLETED.value
    ).count()
    job.completed_clips = completed
    job.progress_percent = (completed / job.total_clips * 100) if job.total_clips > 0 else 0
    if completed >= job.total_clips:
        job.status = JobStatus.COMPLETED.value
        job.completed_at = datetime.utcnow()
    
    # Switch backend so frontend shows clips view instead of prompts view
    if attached > 0 and job.backend == 'prompt_only':
        job.backend = 'import'
    
    db.commit()
    
    # Cleanup temp files
    for cd in clip_data:
        try:
            if os.path.exists(cd['temp_path']):
                os.remove(cd['temp_path'])
        except Exception:
            pass
    
    print(f"[AttachClips] ✓ Attached {attached}/{len(clip_data)} clips to job {job_id[:8]}", flush=True)
    
    return {
        "attached": attached,
        "total_slots": len(existing_clips),
        "total_uploaded": len(clip_data),
        "order_by": order_by,
    }


@app.post("/api/jobs/{job_id}/reorder")
async def reorder_clips(
    job_id: str,
    request: dict,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Reorder clips within a job.
    
    Body: {"clip_order": [3, 1, 0, 2, 4]}
    Each value is the clip ID, position in array = new clip_index.
    """
    job = get_user_job(db, job_id, current_user)
    clip_order = request.get("clip_order", [])
    
    if not clip_order:
        raise HTTPException(status_code=400, detail="clip_order is required")
    
    clips = db.query(Clip).filter(Clip.job_id == job_id).all()
    clip_map = {c.id: c for c in clips}
    
    updated = 0
    for new_idx, clip_id in enumerate(clip_order):
        clip = clip_map.get(clip_id)
        if clip:
            clip.clip_index = new_idx
            clip.dialogue_id = new_idx + 1
            updated += 1
    
    db.commit()
    print(f"[Reorder] Job {job_id[:8]}: reordered {updated} clips", flush=True)
    
    return {"status": "ok", "reordered": updated}


# ============ Final Video Export ============

class ExportSettings(BaseModel):
    frames_to_cut_start: int = Field(default=7, ge=0, le=30)
    frames_to_cut_end: int = Field(default=7, ge=0, le=30)
    smart_trim: bool = True  # Don't trim first clip / cut-to scenes
    remove_silence: bool = False
    silence_mode: str = "energy"  # "energy" = ffmpeg silencedetect, "whisper" = speech-based detection
    silence_trigger: float = Field(default=1.5, ge=0.3, le=5.0)   # Gaps >= this are trimmed (seconds)
    silence_keep: float = Field(default=0.3, ge=0.0, le=2.0)       # Silence to preserve at each cut (seconds)
    silence_threshold: float = Field(default=0.75, ge=0.1, le=1.0) # VAD confidence: higher = only clear speech kept
    # Individual audio enhancement toggles
    remove_laughter: bool = False  # noisereduce (treats laughter as noise)
    denoise_strength: float = Field(default=0.75, ge=0.0, le=1.0)
    apply_deepfilter: bool = False  # DeepFilterNet (removes hiss/static)
    apply_voice_filter: bool = False  # Compressor, gate, limiter
    apply_loudnorm: bool = False  # EBU R128 -16 LUFS
    # Master audio alignment (assemble jobs only)
    master_audio_filename: Optional[str] = None  # If set, align clips to this master audio
    max_clip_speed: float = Field(default=1.5, ge=0.9, le=5.0)  # Max speed multiplier for clip alignment (0.9=slight slowdown, 5.0=very fast)
    min_gap_for_black: float = Field(default=2.0, ge=0.0, le=10.0)  # Gaps shorter than this (seconds) are filled by extending the previous clip instead of black
    # Transitions (assemble jobs only)
    transition: str = "none"  # xfade transition type: none, fade, fadeblack, fadewhite, slideleft, slideright, slideup, slidedown, dissolve, circlecrop, wipeleft, wiperight, smoothleft, smoothright, radial, zoomin, pixelize
    transition_duration: float = Field(default=0.5, ge=0.2, le=1.5)
    # Legacy (backwards compatibility)
    playback_speed: float = Field(default=1.0, ge=1.0, le=1.5)  # 1.0 = normal, up to 1.5×
    enhance_audio: bool = False


@app.post("/api/jobs/{job_id}/upload-master-audio")
async def upload_master_audio(
    job_id: str,
    audio: UploadFile = File(...),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Upload a master audio file for an assemble job.
    Saves to the job's output directory for use during export.
    """
    job = get_user_job(db, job_id, current_user)
    
    # Validate it's an assemble/import job
    config = json.loads(job.config_json) if job.config_json else {}
    if not config.get("assembly_mode") and job.backend != "import":
        raise HTTPException(status_code=400, detail="Master audio is only supported for assemble jobs")
    
    # Validate file type
    allowed_ext = {'.mp3', '.wav', '.m4a', '.aac', '.ogg', '.flac', '.mp4'}
    suffix = Path(audio.filename).suffix.lower() if audio.filename else ''
    if suffix not in allowed_ext:
        raise HTTPException(status_code=400, detail=f"Unsupported audio format. Allowed: {', '.join(allowed_ext)}")
    
    # Save to job output dir
    output_dir = Path(job.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    master_filename = f"master_audio{suffix}"
    master_path = output_dir / master_filename
    
    content = await audio.read()
    with open(master_path, "wb") as f:
        f.write(content)
    
    # Also upload to R2 for persistence
    try:
        from backends.storage import is_storage_configured, get_storage
        if is_storage_configured():
            storage = get_storage()
            r2_key = f"jobs/{job_id}/outputs/{master_filename}"
            await asyncio.to_thread(storage.upload_file, str(master_path), r2_key, audio.content_type or 'audio/mpeg')
            print(f"[MasterAudio] Uploaded to R2: {r2_key}", flush=True)
    except Exception as e:
        print(f"[MasterAudio] R2 upload failed (non-fatal): {e}", flush=True)
    
    size_mb = len(content) / (1024 * 1024)
    print(f"[MasterAudio] Saved {master_filename} ({size_mb:.1f}MB) for job {job_id[:8]}", flush=True)
    
    return {"filename": master_filename, "size_bytes": len(content)}


async def _extract_and_upload_audio(video_path: Path, job_id: str, video_filename: str) -> dict:
    """Extract audio from a video file, save as MP3, upload to R2.
    
    Returns: {audio_filename, audio_download_url} or {} on failure.
    Called after every export/voice-clone to provide a separate audio download.
    """
    try:
        import subprocess
        audio_filename = video_filename.rsplit('.', 1)[0] + '.mp3'
        audio_path = video_path.parent / audio_filename
        
        # Extract audio as MP3 (192k, standard quality)
        cmd = [
            "ffmpeg", "-y", "-i", str(video_path),
            "-vn", "-c:a", "libmp3lame", "-b:a", "192k",
            "-map", "0:a:0",
            str(audio_path)
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=120)
        
        if result.returncode != 0 or not audio_path.exists():
            print(f"[AudioExtract] FFmpeg failed: {result.stderr[:200] if result.stderr else 'no output'}", flush=True)
            return {}
        
        print(f"[AudioExtract] Extracted: {audio_filename} ({audio_path.stat().st_size / 1024:.0f}KB)", flush=True)
        
        # Upload to R2
        try:
            from backends.storage import is_storage_configured, get_storage
            if is_storage_configured():
                storage = get_storage()
                r2_key = f"jobs/{job_id}/outputs/{audio_filename}"
                await asyncio.to_thread(storage.upload_file, str(audio_path), r2_key, 'audio/mpeg')
                print(f"[AudioExtract] Uploaded to R2: {r2_key}", flush=True)
        except Exception as e:
            print(f"[AudioExtract] R2 upload failed (non-fatal): {e}", flush=True)
        
        return {
            "audio_filename": audio_filename,
            "audio_download_url": f"/api/jobs/{job_id}/outputs/{audio_filename}",
        }
    except Exception as e:
        print(f"[AudioExtract] Failed (non-fatal): {e}", flush=True)
        return {}


@app.post("/api/jobs/{job_id}/export-final")
async def export_final_video(
    job_id: str,
    settings: ExportSettings,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Export all approved clips as a single final video.
    Optionally applies trimming and Voice Activity Detection (VAD).
    
    Works even after server restart by falling back to filesystem.
    
    Rules for start frame trimming:
    - Never trim start frames from the FIRST clip (clip_index 0)
    - Never trim start frames from clips that start a "cut" transition scene
    """
    from video_processor import export_final_video as process_export, check_vad_available
    
    job = get_user_job(db, job_id, current_user)
    
    # Determine output directory
    output_dir = Path(job.output_dir)
    dialogue_json = job.dialogue_json
    
    # Get approved clips from database — respect lineup override if set
    clip_info = []
    cut_scene_first_clips = set()
    
    if job.clip_order_json:
        # Custom lineup order
        try:
            lineup_ids = json.loads(job.clip_order_json)
            all_clips = db.query(Clip).filter(
                Clip.job_id == job_id,
                Clip.status == ClipStatus.COMPLETED.value,
            ).all()
            clip_map = {c.id: c for c in all_clips}
            clips = [clip_map[cid] for cid in lineup_ids if cid in clip_map]
            print(f"[Export] Using custom lineup order: {len(clips)} clips", flush=True)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[Export] Lineup parse error, falling back to default: {e}", flush=True)
            clips = db.query(Clip).filter(
                Clip.job_id == job_id,
                Clip.approval_status == "approved"
            ).order_by(Clip.clip_index).all()
    else:
        clips = db.query(Clip).filter(
            Clip.job_id == job_id,
            Clip.approval_status == "approved"
        ).order_by(Clip.clip_index).all()
    
    if not clips:
        raise HTTPException(status_code=400, detail="No approved clips to export")
    
    # Parse scenes for smart trim
    try:
        dialogue_data = json.loads(dialogue_json) if dialogue_json else {}
        scenes = dialogue_data.get("scenes", [])
        
        if scenes and settings.smart_trim:
            for scene in scenes:
                transition = scene.get("transition", None)
                scene_clips = scene.get("clips", [])
                if transition == "cut" and scene_clips:
                    first_clip_of_scene = min(scene_clips)
                    cut_scene_first_clips.add(first_clip_of_scene)
                    print(f"[Export] Scene with 'cut' transition starts at clip {first_clip_of_scene}")
    except Exception as e:
        print(f"[Export] Warning: Could not parse scenes: {e}")
    
    # Import storage helper
    from backends.storage import is_storage_configured, get_storage
    storage = None
    if is_storage_configured():
        try:
            storage = get_storage()
        except Exception as e:
            print(f"[Export] Storage init warning: {e}")
    
    # Collect clip file paths - download from R2 in parallel (I/O bound, safe to parallelize)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Use lineup position (enumerate order) as the sort key, not clip_index
    # This ensures custom lineup order is preserved through parallel download
    _has_lineup = job.clip_order_json is not None

    def _download_clip(pos_clip):
        pos, clip = pos_clip
        # v681 — text-card clips have NO Veo render and NO output_filename.
        # Emit a synthetic clip_info entry that the video_processor's
        # _trim_one branch will render via render_text_card directly. The
        # placeholder path is never read — the text_card branch in
        # video_processor.py:_trim_one returns the rendered file before
        # any path access. Use temp_dir-style placeholder so it's clearly
        # synthetic if anything inspects it.
        if (clip.scene_type or "").lower() == "text_card":
            # v681 — text_card scenes use clip.target_duration_s as the
            # rendered duration (Clip writer overloads the field for
            # text_card; see DialogueLineInput.duration_s comment).
            # Falls back to 1.0s when the author didn't specify a duration.
            tc_duration = float(clip.target_duration_s) if clip.target_duration_s else 1.0
            return {
                "path": output_dir / f"_text_card_{clip.clip_index:04d}.mp4",
                "clip_index": clip.clip_index,
                "skip_start_trim": True,
                "dialogue_text": "",
                "cut_mode": None,
                "target_duration_s": None,
                "scene_type": "text_card",
                "caption": clip.caption or "",
                "bg_color": clip.bg_color or "black",
                "duration_s": tc_duration,
                "_order": pos,
            }
        if not clip.output_filename:
            return None
        clip_path = output_dir / clip.output_filename
        if not clip_path.exists() and storage:
            try:
                r2_key = f"jobs/{job_id}/outputs/{clip.output_filename}"
                if storage.exists(r2_key):
                    print(f"[Export] Downloading clip {clip.clip_index} from R2: {clip.output_filename}")
                    storage.download_file(r2_key, str(clip_path))
            except Exception as e:
                print(f"[Export] R2 download error for clip {clip.clip_index}: {e}")
        if clip_path.exists():
            skip_start_trim = False
            if settings.smart_trim:
                # For lineup override: only skip trim on first clip in lineup
                if _has_lineup:
                    skip_start_trim = (pos == 0)
                else:
                    skip_start_trim = (clip.clip_index == 0 or clip.clip_index in cut_scene_first_clips)
            return {
                "path": clip_path,
                "clip_index": clip.clip_index,
                "skip_start_trim": skip_start_trim,
                "dialogue_text": clip.dialogue_text or "",
                # v667/v668 — propagate cut_mode + target_duration_s so the
                # video_processor can branch trim/VAD strategy per clip.
                "cut_mode": clip.cut_mode,
                "target_duration_s": clip.target_duration_s,
                # v681 — text-card / caption denorm (NULL for shot clips,
                # so the text_card branch in _trim_one is a no-op for them).
                "scene_type": clip.scene_type,
                "caption": clip.caption,
                "bg_color": clip.bg_color,
                "_order": pos
            }
        return None

    from concurrent.futures import ThreadPoolExecutor as _TPE
    print(f"[Export] Downloading {len(clips)} clips from R2 in parallel (3 workers)...")
    with _TPE(max_workers=3) as pool:
        results = list(pool.map(_download_clip, list(enumerate(clips))))

    # Sort by clip_index to preserve order, filter None
    clip_info = sorted(
        [r for r in results if r is not None],
        key=lambda x: x["_order"]
    )
    for r in clip_info:
        del r["_order"]
        if r.get("skip_start_trim"):
            print(f"[Export] Clip {r['clip_index']}: SKIP start frame trim")
    
    # Check VAD availability if requested
    if settings.remove_silence and not check_vad_available():
        raise HTTPException(
            status_code=400,
            detail="VAD requires torch and numpy. Install with: pip install torch numpy"
        )
    
    if not clip_info:
        # Debug: Log what we tried
        for clip in clips:
            print(f"[Export] DEBUG: Clip {clip.clip_index} output_filename={clip.output_filename}, approval_status={clip.approval_status}")
        print(f"[Export] DEBUG: Storage configured={is_storage_configured() if 'is_storage_configured' in dir() else 'N/A'}, storage={storage is not None}")
        raise HTTPException(status_code=400, detail="No valid clip files found")
    
    print(f"[Export] Smart trim: {settings.smart_trim}, Start frames: {settings.frames_to_cut_start}, End frames: {settings.frames_to_cut_end}, Remove silence: {settings.remove_silence} ({settings.silence_mode}), Remove laughter: {settings.remove_laughter}, DeepFilter: {settings.apply_deepfilter}, Loudnorm: {settings.apply_loudnorm}, Speed: {settings.playback_speed}, Master audio: {settings.master_audio_filename}")
    
    # Create output filename with unique suffix to prevent collisions
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_suffix = uuid.uuid4().hex[:6]  # 6 char random suffix
    output_filename = f"final_export_{timestamp}_{unique_suffix}.mp4"
    output_path = output_dir / output_filename
    
    try:
        print(f"[Export] Starting export for job {job_id}")
        print(f"[Export] Clips to process: {len(clip_info)}")
        print(f"[Export] Output path: {output_path}")

        # v539 — Prefix Short Lines: when the user enabled the prefix
        # toggle at job-creation time, Veo was prompted to speak each
        # short line with the prefix word prepended (e.g. "only this is
        # your problem"). The WhisperVAD post-processor uses the
        # dialogue script to identify which transcribed words are speech
        # vs filler — so the script we hand it MUST also include the
        # prefix word, otherwise the matcher treats "only" as filler and
        # silence-removal cuts it out of the final export. This is the
        # same class of silent-failure bug as the v517 voiceover
        # phrase-match misroute, hence the explicit comment.
        try:
            from veo_generator import maybe_prefix_short_dialogue
            _job_cfg_for_prefix = json.loads(job.config_json) if job.config_json else {}
            _prefix_enabled = bool(_job_cfg_for_prefix.get("prefix_short_enabled", False))
            _prefix_word = _job_cfg_for_prefix.get("prefix_short_word", "only")
            _prefix_threshold = int(_job_cfg_for_prefix.get("prefix_short_threshold", 15))
        except Exception as _e:
            print(f"[Export] Prefix-short config load warning: {_e}")
            _prefix_enabled = False
            _prefix_word = "only"
            _prefix_threshold = 15

        def _apply_prefix(line: str) -> str:
            try:
                return maybe_prefix_short_dialogue(
                    line or "", _prefix_enabled, _prefix_word, _prefix_threshold
                )
            except Exception:
                return line or ""

        # === Master Audio Alignment (assemble jobs only) ===
        if settings.master_audio_filename:
            from video_processor import export_with_master_audio
            
            master_path = output_dir / settings.master_audio_filename
            
            # Recover from R2 if not on disk
            if not master_path.exists() and storage:
                try:
                    r2_key = f"jobs/{job_id}/outputs/{settings.master_audio_filename}"
                    if storage.exists(r2_key):
                        print(f"[Export] Recovering master audio from R2: {r2_key}")
                        await asyncio.to_thread(storage.download_file, r2_key, str(master_path))
                except Exception as e:
                    print(f"[Export] Master audio R2 recovery failed: {e}")
            
            if not master_path.exists():
                raise HTTPException(status_code=400, detail="Master audio file not found. Please re-upload it.")
            
            # Get dialogue lines from clips (in order)
            # v539: apply Prefix Short Lines if enabled — the master
            # audio aligner uses these strings to fingerprint each clip
            # against the master track, so they must reflect what was
            # actually spoken in the rendered Veo audio.
            dialogue_lines = []
            for clip in clips:
                dialogue_lines.append(_apply_prefix(clip.dialogue_text or ""))
            
            print(f"[Export] Master audio alignment: {settings.master_audio_filename}")
            print(f"[Export] Dialogue lines: {len(dialogue_lines)}")
            
            stats = await asyncio.to_thread(
                export_with_master_audio,
                clip_info=clip_info,
                dialogue_lines=dialogue_lines,
                master_audio_path=master_path,
                output_path=output_path,
                frames_to_cut_start=settings.frames_to_cut_start,
                frames_to_cut_end=settings.frames_to_cut_end,
                transition=settings.transition,
                transition_duration=settings.transition_duration,
                max_clip_speed=settings.max_clip_speed,
                min_gap_for_black=settings.min_gap_for_black,
            )
        else:
            # === Regular Export (no master audio) ===
            # Process the export with per-clip trim settings (non-blocking)
            stats = await asyncio.to_thread(
                process_export,
                clip_info=clip_info,
                output_path=output_path,
                frames_to_cut_start=settings.frames_to_cut_start,
                frames_to_cut_end=settings.frames_to_cut_end,
                remove_silence=settings.remove_silence,
                silence_mode=settings.silence_mode,
                vad_threshold=settings.silence_threshold,
                silence_trigger=settings.silence_trigger,
                silence_keep=settings.silence_keep,
                transition=settings.transition,
                transition_duration=settings.transition_duration,
                # v553 — pass the user's ORIGINAL dialogue line to the
                # matcher, NOT the prefixed version. Earlier versions
                # passed `_apply_prefix(line)` so the matcher's script
                # anchored on the "only" prefix word, and v542 then
                # tried to drop "only" from the matched output. That
                # was brittle: depending on punctuation, prefix flags,
                # and whisper-timing edge cases, "only" leaked into
                # the kept audio (clips 2/3/4/5 of the Nuri ED export).
                #
                # The cleaner approach: keep prefix-short ON at PROMPT
                # BUILD time (Veo still gets "only [line]" to speak,
                # which gives it a clean-onset throwaway word), but
                # tell the matcher only about the user's actual line.
                # Veo's spoken "only" then becomes an unmatched whisper
                # word that lives BEFORE the cursor's first match. The
                # in-order matcher (line 1070) walks the user's words,
                # finds "pick"/"what"/"you"/"and" past the "only", and
                # cursor advances past it. "only" is in unmatched_words,
                # the v549 padding clamp pushes the segment start past
                # it, and the export contains only the user's line.
                #
                # The v542 prefix-drop in _match_whisper_to_dialogue is
                # left in place but becomes a no-op (never triggers
                # because clip_words[0] is no longer the prefix word).
                # Kept for backward compatibility — harmless if never
                # fired.
                dialogue_texts=[c.get("dialogue_text", "") or "" for c in clip_info],
                language=json.loads(job.config_json).get("language", "English") if job.config_json else "English",
                # v553 — kept for back-compat. v542 prefix-drop is
                # now a no-op because the matcher's script no longer
                # contains the prefix word.
                cut_prefix_audio=False,
                prefix_word=_prefix_word,
            )
        
        print(f"[Export] Success! Stats: {stats}")
        
        # Apply audio enhancement if any audio toggle is enabled
        any_audio_enabled = settings.remove_laughter or settings.apply_deepfilter or settings.apply_voice_filter or settings.apply_loudnorm
        
        if any_audio_enabled:
            try:
                enabled_steps = []
                if settings.remove_laughter: enabled_steps.append(f"laughter({settings.denoise_strength})")
                if settings.apply_deepfilter: enabled_steps.append("deepfilter")
                if settings.apply_voice_filter: enabled_steps.append("voicefilter")
                if settings.apply_loudnorm: enabled_steps.append("loudnorm")
                print(f"[Export] Applying audio enhancement: {', '.join(enabled_steps)}")
                
                # Enhance the exported video
                enhanced_path = output_dir / f"enhanced_{output_filename}"
                
                from audio_processor import enhance_audio
                audio_stats = await asyncio.to_thread(
                    enhance_audio,
                    output_path,
                    enhanced_path,
                    remove_laughter=settings.remove_laughter,
                    denoise_strength=settings.denoise_strength,
                    apply_deepfilter=settings.apply_deepfilter,
                    apply_voice_filter=settings.apply_voice_filter,
                    apply_loudnorm=settings.apply_loudnorm
                )
                
                if audio_stats.get("enhanced"):
                    # Replace original with enhanced
                    import os
                    os.replace(enhanced_path, output_path)
                    stats["audio_enhanced"] = True
                    stats["audio_stats"] = audio_stats
                    print(f"[Export] Audio enhancement applied: {audio_stats}")
                else:
                    print(f"[Export] Audio enhancement skipped: {audio_stats.get('reason')}")
                    stats["audio_enhanced"] = False
            except Exception as e:
                print(f"[Export] Audio enhancement failed (non-fatal): {e}")
                import traceback
                traceback.print_exc()
                stats["audio_enhanced"] = False
                stats["audio_error"] = str(e)
        
        # Apply playback speed if > 1.0 (skip if master audio alignment was used — speeds already handled per-clip)
        print(f"[Export] Speed check: playback_speed={settings.playback_speed}, master_audio={settings.master_audio_filename}, will_apply={settings.playback_speed and settings.playback_speed > 1.01 and not settings.master_audio_filename}", flush=True)
        if settings.playback_speed and settings.playback_speed > 1.01 and not settings.master_audio_filename:
            try:
                speed = round(settings.playback_speed, 3)
                sped_filename = f"sped_{output_filename}"
                sped_path = output_dir / sped_filename
                # setpts=(1/speed)*PTS speeds up video; atempo handles audio (max 2.0, min 0.5)
                #
                # v597: force constant 24fps output via "-r 24 -vsync cfr".
                # Same bug v560 fixed in master_align: setpts=PTS/N adjusts
                # presentation timestamps but ffmpeg keeps the original frame
                # count, producing a variable-framerate output. Container says
                # X seconds but internal packet timestamps span the original
                # (longer) duration. Visible to the user as "tweaking frames"
                # / micro-stutter at playback because the player's frame-pacing
                # doesn't match the encoded packet timing. This is the same
                # failure mode as v560 in master_align (which already has the
                # CFR fix); the export-speed path was missed and silently
                # regressed for any clip exported with playback_speed > 1.0.
                import subprocess as _sp
                # v631 — replace `-r 24 -vsync cfr` with in-filter `fps=24`.
                # User report: "a frame from clip 7 appears at output 13-14s
                # that shouldn't be there. Not extra frames after 'day' —
                # looks like the last frame of clip 7 in the wrong position."
                #
                # Root cause: `-vsync cfr` does dup/drop at the ENCODER
                # boundary based on PTS rounding. With setpts=PTS/1.1 on a
                # 24fps CFR input, the pre-speed boundary between seg7 (last
                # frame at PTS 49.917s in source) and seg8 (first frame at
                # 53.958s in source) becomes adjacent in the concatenated
                # file. After setpts, the speed-output 24fps grid samples
                # near this boundary using `nearest input frame ≤ output
                # PTS`. Sub-millisecond float rounding can pick seg7's last
                # frame for one extra output slot whose audio already belongs
                # to seg8 → user sees a 1-frame freeze of clip 7 visible
                # during clip 8's audio onset.
                #
                # The `fps=24` filter inside the graph uses
                # round-half-to-even on PTS sampling and resolves frame
                # selection BEFORE the encoder sees anything. No encoder
                # boundary dup. No PTS-rounding ghosts at concat seams.
                # Also bumps setpts precision from 6 → 9 decimals to match
                # v629 trim precision.
                cmd_speed = [
                    "ffmpeg", "-y", "-i", str(output_path),
                    "-filter_complex",
                    f"[0:v]setpts={1/speed:.9f}*PTS,fps=24[v];[0:a]atempo={speed:.6f}[a]",
                    "-map", "[v]", "-map", "[a]",
                    "-c:v", "libx264", "-preset", "ultrafast", "-crf", "18",
                    "-c:a", "aac", "-b:a", "192k",
                    str(sped_path)
                ]
                result = _sp.run(cmd_speed, capture_output=True, timeout=300)
                if result.returncode == 0:
                    import os as _os
                    # v633 — diagnostic: count frames in pre-speed and
                    # post-speed files to localize the ghost-frame source.
                    try:
                        cnt_pre_cmd = ["ffmpeg", "-i", str(output_path),
                                       "-map", "0:v:0", "-c", "copy",
                                       "-f", "null", "-"]
                        cnt_post_cmd = ["ffmpeg", "-i", str(sped_path),
                                        "-map", "0:v:0", "-c", "copy",
                                        "-f", "null", "-"]
                        import re as _re
                        def _count(cmd):
                            r = _sp.run(cmd, capture_output=True, timeout=60)
                            text = r.stderr.decode("utf-8", errors="ignore")
                            for line in text.splitlines()[::-1]:
                                m = _re.search(r"frame=\s*(\d+)", line)
                                if m:
                                    return int(m.group(1))
                            return None
                        pre_n = _count(cnt_pre_cmd)
                        post_n = _count(cnt_post_cmd)
                        expected_post = round(pre_n / speed) if pre_n else None
                        delta = (post_n - expected_post) if (post_n and expected_post) else None
                        print(f"[v633] PRE-speed: {pre_n} frames | "
                              f"POST-speed: {post_n} frames | "
                              f"expected POST = round({pre_n}/{speed}) = {expected_post} | "
                              f"delta = {delta:+d}" if delta is not None else
                              f"[v633] PRE={pre_n} POST={post_n} expected={expected_post}",
                              flush=True)
                        if delta is not None and delta != 0:
                            print(f"[v633] ⚠ POST-SPEED FRAME COUNT MISMATCH: "
                                  f"{delta:+d} frames vs expected. "
                                  f"Speed pass is producing extra/missing frames.",
                                  flush=True)
                    except Exception as _e:
                        print(f"[v633] post-speed diag failed (non-fatal): {_e}", flush=True)
                    _os.replace(sped_path, output_path)
                    stats["playback_speed"] = speed
                    print(f"[Export] Speed applied: {speed}×", flush=True)
                else:
                    print(f"[Export] Speed change failed: {result.stderr.decode()[:200]}", flush=True)
            except Exception as e:
                print(f"[Export] Speed change error (non-fatal): {e}", flush=True)

        # Upload to R2 for persistence (voice swap needs this as input after Render restarts)
        try:
            from backends.storage import is_storage_configured, get_storage
            if is_storage_configured():
                storage = get_storage()
                r2_key = f"jobs/{job_id}/outputs/{output_filename}"
                await asyncio.to_thread(storage.upload_file, str(output_path), r2_key, 'video/mp4')
                print(f"[Export] Uploaded to R2: {r2_key}", flush=True)
        except Exception as e:
            print(f"[Export] R2 upload failed (non-fatal): {e}", flush=True)

        # Extract audio-only file alongside the video (skip for assemble jobs — they use master audio)
        audio_info = {}
        if job.backend != 'import':
            audio_info = await _extract_and_upload_audio(output_path, job_id, output_filename)

        # Mark job as exported
        job.has_export = True
        db.commit()

        return {
            "success": True,
            "filename": output_filename,
            "download_url": f"/api/jobs/{job_id}/outputs/{output_filename}",
            "stats": stats,
            **audio_info,
        }
        
    except Exception as e:
        import traceback
        print(f"[Export] ERROR: {str(e)}")
        print(f"[Export] Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@app.get("/api/vad-available")
async def check_vad_availability():
    """Check if VAD dependencies are installed."""
    from video_processor import check_vad_available
    return {"available": check_vad_available()}


@app.get("/api/audio-enhance-available")
async def check_audio_enhance_availability():
    """Check if audio enhancement dependencies are installed."""
    try:
        import numpy
        import soundfile
        import noisereduce
        return {"available": True}
    except ImportError:
        return {"available": False}


@app.get("/api/jobs/{job_id}/export-audio/{filename}")
async def export_audio_from_video(
    job_id: str,
    filename: str,
    enhance: bool = True,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Export audio from a video file as WAV.
    Use this to download audio for external processing (OpenVoice, ElevenLabs).
    
    Args:
        job_id: Job ID
        filename: Video filename (e.g., "final_export_xxx.mp4")
        enhance: Apply basic noise reduction before export
    """
    from audio_processor import export_audio_only
    
    job = get_user_job(db, job_id, current_user)
    output_dir = Path(job.output_dir)
    
    video_path = output_dir / filename
    if not video_path.exists():
        # Try R2 recovery
        try:
            from backends.storage import is_storage_configured, get_storage
            if is_storage_configured():
                storage = get_storage()
                r2_key = f"jobs/{job_id}/outputs/{filename}"
                if storage.exists(r2_key):
                    output_dir.mkdir(parents=True, exist_ok=True)
                    await asyncio.to_thread(storage.download_file, r2_key, str(video_path))
                    print(f"[ExportAudio] Recovered source video from R2: {filename}", flush=True)
        except Exception as e:
            print(f"[ExportAudio] R2 recovery failed: {e}", flush=True)
    
    if not video_path.exists():
        raise HTTPException(status_code=404, detail="Video file not found (try re-exporting)")
    
    # Create audio output path
    audio_filename = f"{video_path.stem}_audio.wav"
    audio_path = output_dir / audio_filename
    
    try:
        success = export_audio_only(video_path, audio_path, enhance=enhance)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to export audio")
        
        return FileResponse(
            audio_path,
            media_type="audio/wav",
            filename=audio_filename
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Audio export failed: {str(e)}")


@app.post("/api/jobs/{job_id}/import-audio/{video_filename}")
async def import_audio_to_video(
    job_id: str,
    video_filename: str,
    audio_file: UploadFile = File(...),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Import external audio into a video (replace existing audio).
    Use this after processing audio with OpenVoice, ElevenLabs, etc.
    
    Args:
        job_id: Job ID
        video_filename: Original video filename to replace audio in
        audio_file: New audio file (WAV or MP3)
    
    Returns:
        New video file with replaced audio
    """
    from audio_processor import import_audio
    
    job = get_user_job(db, job_id, current_user)
    output_dir = Path(job.output_dir)
    
    video_path = output_dir / video_filename
    if not video_path.exists():
        # Try R2 recovery
        try:
            from backends.storage import is_storage_configured, get_storage
            if is_storage_configured():
                storage = get_storage()
                r2_key = f"jobs/{job_id}/outputs/{video_filename}"
                if storage.exists(r2_key):
                    output_dir.mkdir(parents=True, exist_ok=True)
                    await asyncio.to_thread(storage.download_file, r2_key, str(video_path))
                    print(f"[ImportAudio] Recovered source video from R2: {video_filename}", flush=True)
        except Exception as e:
            print(f"[ImportAudio] R2 recovery failed: {e}", flush=True)
    
    if not video_path.exists():
        raise HTTPException(status_code=404, detail="Video file not found (try re-exporting)")
    
    # Save uploaded audio
    audio_ext = Path(audio_file.filename).suffix or ".wav"
    temp_audio = output_dir / f"temp_imported_audio{audio_ext}"
    
    try:
        # Save uploaded file
        content = await audio_file.read()
        with open(temp_audio, "wb") as f:
            f.write(content)
        
        # Create output with new audio
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_suffix = uuid.uuid4().hex[:6]
        output_filename = f"voice_swapped_{timestamp}_{unique_suffix}.mp4"
        output_path = output_dir / output_filename
        
        success = import_audio(video_path, temp_audio, output_path)
        
        # Cleanup temp file
        if temp_audio.exists():
            temp_audio.unlink()
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to import audio")
        
        # Upload to R2 for persistence
        try:
            from backends.storage import is_storage_configured, get_storage
            if is_storage_configured():
                storage = get_storage()
                r2_key = f"jobs/{job_id}/outputs/{output_filename}"
                await asyncio.to_thread(storage.upload_file, str(output_path), r2_key, 'video/mp4')
                print(f"[ImportAudio] Uploaded to R2: {r2_key}", flush=True)
        except Exception as e:
            print(f"[ImportAudio] R2 upload failed (non-fatal): {e}", flush=True)
        
        # Extract audio-only file alongside the video
        audio_info = await _extract_and_upload_audio(output_path, job_id, output_filename)

        return {
            "success": True,
            "filename": output_filename,
            "download_url": f"/api/jobs/{job_id}/outputs/{output_filename}",
            **audio_info,
        }
        
    except Exception as e:
        if temp_audio.exists():
            temp_audio.unlink()
        raise HTTPException(status_code=500, detail=f"Audio import failed: {str(e)}")


@app.get("/api/voice-clone-available")
async def check_voice_clone_availability():
    """Check if voice cloning (Replicate) is configured"""
    from voice_cloner import check_replicate_available
    return check_replicate_available()


@app.post("/api/voice-clone-warmup")
async def warmup_voice_clone():
    """
    Trigger warmup of voice clone server (Modal).
    Call this early (e.g., when Export Final is clicked) so the server is warm
    by the time the user wants to voice clone.
    Also warms up DeepFilter Modal endpoint.
    """
    import asyncio
    
    async def warmup_openvoice():
        try:
            from voice_cloner import check_openvoice_available
            result = await asyncio.to_thread(check_openvoice_available)
            print(f"[Warmup] OpenVoice: {result.get('message', 'unknown')}", flush=True)
        except Exception as e:
            print(f"[Warmup] OpenVoice warmup failed: {e}", flush=True)

    async def warmup_deepfilter():
        try:
            import requests as _req, os as _os
            modal_url = _os.environ.get(
                "DEEPFILTER_MODAL_URL",
                "https://kaveno-biz--deepfilter-denoiser-denoise-endpoint.modal.run"
            )
            # Send a tiny ping to wake up the Modal container
            _req.post(modal_url, json={"audio_base64": "", "sample_rate": 48000}, timeout=5)
            print("[Warmup] DeepFilter: ping sent", flush=True)
        except Exception as e:
            print(f"[Warmup] DeepFilter ping: {e}", flush=True)  # expected — just waking it up
    
    # Fire and forget - don't wait for warmup to complete
    asyncio.create_task(warmup_openvoice())
    asyncio.create_task(warmup_deepfilter())
    
    return {"status": "warmup_initiated"}


@app.get("/api/jobs/{job_id}/list-outputs")
async def list_job_outputs(
    job_id: str, 
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """List all output files for a job (local + R2)"""
    job = get_user_job(db, job_id, current_user)
    output_dir = Path(job.output_dir)
    
    # Collect local files
    local_files = set()
    if output_dir.exists():
        for f in output_dir.iterdir():
            if f.is_file():
                local_files.add(f.name)
    
    # Also check R2 for exports/voice-cloned files that may have survived a redeploy
    r2_files = set()
    try:
        from backends.storage import is_storage_configured, get_storage
        if is_storage_configured():
            storage = get_storage()
            r2_prefix = f"jobs/{job_id}/outputs/"
            # list_objects returns keys under the prefix
            if hasattr(storage, 'list_objects'):
                keys = storage.list_objects(r2_prefix)
                for key in keys:
                    filename = key.replace(r2_prefix, "")
                    if filename:
                        r2_files.add(filename)
            elif hasattr(storage, 'client'):
                # Direct S3/R2 listing fallback
                resp = storage.client.list_objects_v2(
                    Bucket=storage.bucket_name,
                    Prefix=r2_prefix,
                    MaxKeys=200
                )
                for obj in resp.get("Contents", []):
                    filename = obj["Key"].replace(r2_prefix, "")
                    if filename:
                        r2_files.add(filename)
    except Exception as e:
        print(f"[ListOutputs] R2 listing failed (non-fatal): {e}", flush=True)
    
    all_files = sorted(local_files | r2_files)
    return {"files": all_files}


class VoiceSwapRequest(BaseModel):
    video_filename: str


@app.post("/api/jobs/{job_id}/voice-swap")
async def voice_swap_video_endpoint(
    job_id: str,
    video_filename: str = Form(...),
    voice_sample: UploadFile = File(None),
    reference_clips: str = Form(None),  # JSON array of clip filenames
    tau: str = Form("0.3"),  # Voice similarity (0.1-0.5, lower = more similar)
    pitch_normalize: str = Form("0.0"),  # Pitch normalization (0.0-1.0, 0 = off)
    provider: str = Form("openvoice"),  # "openvoice" or "elevenlabs"
    elevenlabs_api_key: str = Form(None),
    elevenlabs_voice_id: str = Form(None),
    elevenlabs_stability: str = Form("0.5"),
    elevenlabs_similarity: str = Form("0.75"),
    elevenlabs_style: str = Form("0"),
    elevenlabs_remove_noise: str = Form("true"),
    elevenlabs_speaker_boost: str = Form("true"),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """
    Swap voice in video using AI voice cloning.
    
    Supports two providers:
    - OpenVoice v2: Self-hosted (~$0.01/run)
    - ElevenLabs: Premium quality (uses your API credits)
    
    Args:
        job_id: Job ID
        video_filename: Video file to process
        voice_sample: Reference voice audio file (OpenVoice only)
        reference_clips: OR use clips' audio as reference (OpenVoice only)
        tau: Voice similarity for OpenVoice
        pitch_normalize: Pitch compression for OpenVoice
        provider: "openvoice" or "elevenlabs"
        elevenlabs_api_key: Your ElevenLabs API key (ElevenLabs only)
        elevenlabs_voice_id: Target voice ID (ElevenLabs only)
    
    Returns:
        New video file with cloned voice
    """
    from audio_processor import extract_audio, concatenate_audio_files, replace_audio
    
    job = get_user_job(db, job_id, current_user)
    output_dir = Path(job.output_dir)
    
    video_path = output_dir / video_filename
    if not video_path.exists():
        # Try R2 recovery — Render ephemeral storage may have lost the file
        try:
            from backends.storage import is_storage_configured, get_storage
            if is_storage_configured():
                storage = get_storage()
                r2_key = f"jobs/{job_id}/outputs/{video_filename}"
                if storage.exists(r2_key):
                    output_dir.mkdir(parents=True, exist_ok=True)
                    await asyncio.to_thread(storage.download_file, r2_key, str(video_path))
                    print(f"[VoiceSwap] Recovered source video from R2: {video_filename}", flush=True)
        except Exception as e:
            print(f"[VoiceSwap] R2 recovery failed: {e}", flush=True)
    
    if not video_path.exists():
        raise HTTPException(status_code=404, detail="Video file not found (try re-exporting)")
    
    # Route to appropriate provider
    if provider == "elevenlabs":
        el_settings = {
            "stability": float(elevenlabs_stability),
            "similarity_boost": float(elevenlabs_similarity),
            "style": float(elevenlabs_style),
            "use_speaker_boost": elevenlabs_speaker_boost.lower() == "true",
            "remove_background_noise": elevenlabs_remove_noise.lower() == "true"
        }
        result = await voice_swap_elevenlabs(
            job_id, video_path, output_dir, 
            elevenlabs_api_key, elevenlabs_voice_id, el_settings
        )
    else:
        result = await voice_swap_openvoice(
            job_id, video_path, output_dir,
            voice_sample, reference_clips, 
            float(tau), float(pitch_normalize)
        )
    
    # Mark job as voice-cloned on success
    if result and result.get("success"):
        job.has_voice_clone = True
        db.commit()
    
    return result


async def voice_swap_elevenlabs(
    job_id: str, 
    video_path: Path, 
    output_dir: Path,
    api_key: str, 
    voice_id: str,
    settings: dict = None
):
    """Handle ElevenLabs speech-to-speech voice swap"""
    import httpx
    from audio_processor import extract_audio, replace_audio
    
    if not api_key:
        raise HTTPException(status_code=400, detail="ElevenLabs API key required")
    if not voice_id:
        raise HTTPException(status_code=400, detail="ElevenLabs Voice ID required")
    
    # Default settings
    if settings is None:
        settings = {
            "stability": 0.5,
            "similarity_boost": 0.75,
            "style": 0.0,
            "use_speaker_boost": True,
            "remove_background_noise": True
        }
    
    temp_audio = output_dir / "temp_source_audio.mp3"
    converted_audio = output_dir / "temp_converted_audio.mp3"
    
    try:
        # Step 1: Extract audio from video as mp3
        print(f"[ElevenLabs] Extracting audio from video...")
        if not extract_audio(video_path, temp_audio, format="mp3"):
            raise HTTPException(status_code=500, detail="Failed to extract audio from video")
        
        print(f"[ElevenLabs] Audio extracted: {temp_audio.stat().st_size} bytes")
        
        # Step 2: Call ElevenLabs Speech-to-Speech API
        print(f"[ElevenLabs] Calling speech-to-speech API for voice: {voice_id}")
        print(f"[ElevenLabs] Settings: stability={settings['stability']}, similarity={settings['similarity_boost']}, style={settings['style']}")
        
        url = f"https://api.elevenlabs.io/v1/speech-to-speech/{voice_id}?output_format=mp3_44100_128"
        
        headers = {
            "xi-api-key": api_key
        }
        
        # Read file content first for async compatibility
        with open(temp_audio, "rb") as f:
            audio_content = f.read()
        
        # Build voice_settings JSON
        voice_settings = {
            "stability": settings["stability"],
            "similarity_boost": settings["similarity_boost"],
            "style": settings["style"],
            "use_speaker_boost": settings["use_speaker_boost"]
        }
        
        files = {
            "audio": ("audio.mp3", audio_content, "audio/mpeg"),
        }
        data = {
            "model_id": "eleven_multilingual_sts_v2",
            "voice_settings": json.dumps(voice_settings),
            "remove_background_noise": str(settings["remove_background_noise"]).lower()
        }
        
        async with httpx.AsyncClient(timeout=300) as client:
            response = await client.post(url, headers=headers, data=data, files=files)
        
        print(f"[ElevenLabs] Response status: {response.status_code}")
        
        if response.status_code == 401:
            raise HTTPException(status_code=401, detail="Invalid ElevenLabs API key")
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Voice ID not found: {voice_id}")
        if response.status_code == 422:
            error_detail = response.text[:300] if response.text else "Validation error"
            raise HTTPException(status_code=422, detail=f"ElevenLabs validation error: {error_detail}")
        if response.status_code != 200:
            error_detail = response.text[:200] if response.text else "Unknown error"
            raise HTTPException(status_code=response.status_code, detail=f"ElevenLabs API error: {error_detail}")
        
        # Save converted audio
        with open(converted_audio, "wb") as f:
            f.write(response.content)
        
        print(f"[ElevenLabs] Received {len(response.content)} bytes of converted audio")
        
        # Step 3: Replace audio in video (non-blocking)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_suffix = uuid.uuid4().hex[:6]
        output_filename = f"voice_cloned_el_{timestamp}_{unique_suffix}.mp4"
        output_path = output_dir / output_filename
        
        success = await asyncio.to_thread(replace_audio, video_path, converted_audio, output_path)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to create output video")
        
        print(f"[ElevenLabs] Success! Output: {output_filename}")
        
        # Upload to R2 for persistence across Render restarts
        try:
            from backends.storage import is_storage_configured, get_storage
            if is_storage_configured():
                storage = get_storage()
                r2_key = f"jobs/{job_id}/outputs/{output_filename}"
                await asyncio.to_thread(storage.upload_file, str(output_path), r2_key, 'video/mp4')
                print(f"[ElevenLabs] Uploaded to R2: {r2_key}", flush=True)
        except Exception as e:
            print(f"[ElevenLabs] R2 upload failed (non-fatal): {e}", flush=True)
        
        # Extract audio-only file alongside the video
        audio_info = await _extract_and_upload_audio(output_path, job_id, output_filename)

        return {
            "success": True,
            "filename": output_filename,
            "download_url": f"/api/jobs/{job_id}/outputs/{output_filename}",
            "provider": "elevenlabs",
            "voice_id": voice_id,
            **audio_info,
        }
        
    finally:
        # Cleanup temp files
        if temp_audio.exists():
            temp_audio.unlink()
        if converted_audio.exists():
            converted_audio.unlink()


async def voice_swap_openvoice(
    job_id: str,
    video_path: Path,
    output_dir: Path,
    voice_sample: UploadFile,
    reference_clips: str,
    tau: float,
    pitch_normalize: float
):
    """Handle OpenVoice voice swap (original logic)"""
    from voice_cloner import check_replicate_available, voice_swap_video_sync
    from audio_processor import extract_audio, concatenate_audio_files, enhance_audio_for_voice_clone
    
    # Check if configured
    status = check_replicate_available()
    if not status["available"]:
        raise HTTPException(
            status_code=503, 
            detail=status.get("message", "OpenVoice endpoint not available")
        )
    
    # Parse reference clips if provided
    clip_filenames = []
    if reference_clips:
        try:
            clip_filenames = json.loads(reference_clips)
            if len(clip_filenames) > 4:
                raise HTTPException(status_code=400, detail="Maximum 4 reference clips allowed")
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid reference_clips format")
    
    # Must have either voice_sample or reference_clips
    if not voice_sample and not clip_filenames:
        raise HTTPException(
            status_code=400, 
            detail="Must provide either voice_sample file or reference_clips array"
        )
    
    temp_voice = None
    temp_audio_files = []
    
    # Initialize storage for R2 recovery
    storage = None
    try:
        from backends.storage import is_storage_configured, get_storage
        if is_storage_configured():
            storage = get_storage()
    except Exception:
        pass
    
    try:
        # Get voice reference - either from upload or from clip audio(s)
        if voice_sample and voice_sample.filename:
            # Use uploaded voice sample
            voice_ext = Path(voice_sample.filename).suffix or ".wav"
            temp_voice = output_dir / f"temp_voice_sample{voice_ext}"
            content = await voice_sample.read()
            with open(temp_voice, "wb") as f:
                f.write(content)
            print(f"[VoiceSwap] Using uploaded voice sample: {voice_sample.filename}")
        elif clip_filenames:
            # Extract audio from each reference clip, concatenate, then enhance once
            print(f"[VoiceSwap] Extracting voice from {len(clip_filenames)} clips")
            
            for i, clip_name in enumerate(clip_filenames):
                clip_path = output_dir / clip_name
                # Recover from R2 if not on disk (common after Render restarts)
                if not clip_path.exists() and storage:
                    try:
                        r2_key = f"jobs/{job_id}/outputs/{clip_name}"
                        if storage.exists(r2_key):
                            print(f"[VoiceSwap] Recovering reference clip from R2: {clip_name}")
                            storage.download_file(r2_key, str(clip_path))
                    except Exception as e:
                        print(f"[VoiceSwap] R2 recovery failed for {clip_name}: {e}")
                if not clip_path.exists():
                    print(f"[VoiceSwap] Warning: Clip not found: {clip_name}")
                    continue
                
                temp_audio = output_dir / f"temp_clip_voice_{i}.wav"
                # Basic extraction only (we'll enhance after combining)
                await asyncio.to_thread(extract_audio, clip_path, temp_audio)
                print(f"[VoiceSwap] Extracted audio from: {clip_name}")
                temp_audio_files.append(temp_audio)
            
            if not temp_audio_files:
                raise HTTPException(status_code=404, detail="No valid reference clips found")
            
            # Concatenate all audio files
            if len(temp_audio_files) == 1:
                combined_audio = temp_audio_files[0]
            else:
                combined_audio = output_dir / "temp_combined_voice_raw.wav"
                await asyncio.to_thread(concatenate_audio_files, temp_audio_files, combined_audio, False)
                print(f"[VoiceSwap] Combined {len(temp_audio_files)} clips into single reference")
            
            # Enhance the combined audio once with DeepFilterNet
            temp_voice = output_dir / "temp_voice_enhanced.wav"
            print(f"[VoiceSwap] Applying DeepFilterNet enhancement to combined voice reference...")
            result = await asyncio.to_thread(
                enhance_audio_for_voice_clone, combined_audio, temp_voice,
                denoise=True, denoise_strength=0.8  # Strong denoise for clean voice reference
            )
            if result.get("enhanced"):
                print(f"[VoiceSwap] Voice reference enhanced successfully (denoise: {result.get('denoise_applied')})")
            else:
                # Fallback to unenhanced if enhancement fails
                temp_voice = combined_audio
                print(f"[VoiceSwap] Enhancement skipped, using raw combined audio")
        
        if not temp_voice or not temp_voice.exists():
            raise HTTPException(status_code=400, detail="Failed to prepare voice reference")
        
        # Create output path with unique suffix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_suffix = uuid.uuid4().hex[:6]
        output_filename = f"voice_cloned_{timestamp}_{unique_suffix}.mp4"
        output_path = output_dir / output_filename
        
        # Run voice swap (non-blocking)
        print(f"[VoiceSwap] Starting voice clone using OpenVoice (tau={tau}, pitch_norm={pitch_normalize})")
        result = await asyncio.to_thread(
            voice_swap_video_sync,
            video_path=video_path,
            reference_voice_path=temp_voice,
            output_path=output_path,
            tau=tau,
            pitch_normalize=pitch_normalize
        )
        
        # Cleanup temp files
        for f in temp_audio_files:
            if f and f.exists():
                f.unlink()
        # Clean combined raw audio if it exists
        combined_raw = output_dir / "temp_combined_voice_raw.wav"
        if combined_raw.exists():
            combined_raw.unlink()
        # Clean enhanced voice file
        if temp_voice and temp_voice.exists() and temp_voice not in temp_audio_files:
            temp_voice.unlink()
        
        if not result.get("success"):
            raise HTTPException(
                status_code=500, 
                detail=f"Voice cloning failed: {result.get('error', 'Unknown error')}"
            )
        
        print(f"[VoiceSwap] Success! Output: {output_filename}")
        
        # Upload to R2 for persistence across Render restarts
        try:
            from backends.storage import is_storage_configured, get_storage
            if is_storage_configured():
                storage = get_storage()
                r2_key = f"jobs/{job_id}/outputs/{output_filename}"
                await asyncio.to_thread(storage.upload_file, str(output_path), r2_key, 'video/mp4')
                print(f"[VoiceSwap] Uploaded to R2: {r2_key}", flush=True)
        except Exception as e:
            print(f"[VoiceSwap] R2 upload failed (non-fatal): {e}", flush=True)
        
        # Extract audio-only file alongside the video
        audio_info = await _extract_and_upload_audio(output_path, job_id, output_filename)

        return {
            "success": True,
            "filename": output_filename,
            "download_url": f"/api/jobs/{job_id}/outputs/{output_filename}",
            "cost_estimate": result.get("cost_estimate", "$0.06"),
            "model_used": result.get("model", "HierSpeech++"),
            **audio_info,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        # Clean up temp files
        for f in temp_audio_files:
            if f and f.exists():
                try:
                    f.unlink()
                except:
                    pass
        # Clean combined raw audio
        combined_raw = output_dir / "temp_combined_voice_raw.wav"
        if combined_raw.exists():
            try:
                combined_raw.unlink()
            except:
                pass
        if temp_voice and temp_voice.exists():
            try:
                temp_voice.unlink()
            except:
                pass
        import traceback
        print(f"[VoiceSwap] ERROR: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Voice swap failed: {str(e)}")


@app.get("/api/jobs/{job_id}/images/{filename}")
async def get_job_image(
    job_id: str,
    filename: str,
    direct: int = 0,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Get an image from a job's images directory (local or R2).

    Uses browser cache (1 hour) and an in-memory LRU cache to avoid
    repeated R2 downloads — thumbnails don't change after generation.

    v527: previously held the dep-injected DB session through R2
    download (5-15s for cold-cache fetches). Under heavy frontend
    polling that exhausted the connection pool. Now: query needed
    job data, release session, do R2 work without DB held.

    v687: PROXY IS NOW DEFAULT. Pre-v687 (v561) returned a 302
    redirect to a presigned R2 URL so the browser fetched bytes
    directly from Cloudflare's edge. Faster (~50-100ms vs
    ~200-2000ms through app server) but breaks for users whose
    network blocks `*.r2.cloudflarestorage.com` (some ISP / firewall
    / VPN configs drop direct R2 connections, browser sees
    ERR_CONNECTION_TIMED_OUT). The redirect was the optimization;
    correctness wins. Now the default streams bytes through the
    app server. The redirect-to-R2 fast path becomes opt-in via
    `?direct=1` for users on networks that can reach R2.

    The in-memory LRU cache + browser cache headers mitigate the
    v561 worker-swamping concern: warm-cache requests are
    sub-millisecond memory hits, not R2 fetches.
    """
    from fastapi.responses import Response

    # v687 — proxy is default. Only redirect when caller opts in
    # via `?direct=1` (e.g. internal admin tool that knows it
    # can reach R2 and wants the speed). Frontend doesn't append
    # this; everyone gets the proxy by default.
    force_proxy = not bool(direct)
    
    job = get_user_job(db, job_id, current_user)
    
    # Capture what we need from the job, then RELEASE the session
    job_images_dir = job.images_dir
    db.close()  # v527: release pool slot before slow I/O
    
    # Determine media type
    suffix = Path(filename).suffix.lower()
    media_types = {'.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp'}
    media_type = media_types.get(suffix, 'image/png')
    
    # Cache headers — thumbnails never change, cache for 1 hour
    cache_headers = {"Cache-Control": "public, max-age=3600, immutable"}
    
    # Method 1: Check local filesystem first
    images_path = safe_images_dir(job_images_dir)
    if images_path:
        filepath = images_path / filename
        if filepath.exists():
            return FileResponse(filepath, media_type=media_type, headers=cache_headers)
    
    # Method 2: Check in-memory cache, then R2
    cache_key = f"{job_id}/{filename}"
    cached = _image_cache.get(cache_key)
    if cached:
        return Response(content=cached, media_type=media_type, headers=cache_headers)
    
    try:
        from backends.storage import is_storage_configured, get_storage
        
        if is_storage_configured():
            storage = get_storage()
            r2_key = f"jobs/{job_id}/frames/{filename}"
            
            if storage.exists(r2_key):
                # v561: redirect to R2 presigned URL instead of blocking
                # the eventloop on a synchronous download. Pre-v561 every
                # job thumbnail on the dashboard was downloaded through
                # the app worker on cold cache (post-deploy / first
                # render), which combined with the image_platform variant
                # downloads to swamp the worker for 30-60s. Browser fetches
                # bytes directly from R2 edge — typically 50-100ms vs
                # 200-2000ms through the app worker. Memory cache and
                # local FileResponse paths above remain unchanged for
                # the warm-cache case.
                #
                # v687 — only redirect on explicit opt-in via ?direct=1.
                # Default (force_proxy=True) falls through to the
                # download-and-stream path below, which works on any
                # network that reaches the app server.
                if not force_proxy:
                    from fastapi.responses import RedirectResponse
                    try:
                        presigned = storage.get_presigned_url(r2_key, expires_in=3600)
                        return RedirectResponse(
                            url=presigned,
                            status_code=302,
                            headers={
                                "Cache-Control": "public, max-age=3600, immutable",
                            },
                        )
                    except Exception as _pe:
                        print(f"[Images] Presigned URL generation failed, falling back to download: {_pe}", flush=True)
                        # fall through to legacy download path

                # Legacy fallback path (only reached if presigned URL
                # generation itself fails — e.g. credentials issue):
                import tempfile
                temp_path = tempfile.mktemp(suffix=suffix)
                storage.download_file(r2_key, temp_path)
                
                with open(temp_path, 'rb') as f:
                    content = f.read()
                
                try:
                    os.remove(temp_path)
                except:
                    pass
                
                # Store in memory cache (up to 200 images, ~50MB max for typical thumbnails)
                _image_cache_put(cache_key, content)
                
                return Response(content=content, media_type=media_type, headers=cache_headers)
    except Exception as e:
        print(f"[Images] Error fetching from R2: {e}", flush=True)
    
    raise HTTPException(status_code=404, detail="Image not found")


# In-memory LRU cache for images (avoids repeated R2 downloads)
_image_cache: Dict[str, bytes] = {}
_image_cache_order: list = []
_IMAGE_CACHE_MAX = 200  # Max entries

def _image_cache_put(key: str, data: bytes):
    """Add to LRU cache, evict oldest if full."""
    if key in _image_cache:
        _image_cache_order.remove(key)
    elif len(_image_cache) >= _IMAGE_CACHE_MAX:
        evict = _image_cache_order.pop(0)
        _image_cache.pop(evict, None)
    _image_cache[key] = data
    _image_cache_order.append(key)


# ============ Script Splitting ============

class ScriptSplitRequest(BaseModel):
    script: str
    language: str = "English"

# Speaking rates by language (words per second) for natural speech
LANGUAGE_SPEAKING_RATES = {
    "English": 2.5,      # ~150 wpm → 17-18 words per 7 sec
    "Italian": 2.8,      # ~168 wpm → 19-20 words per 7 sec  
    "Spanish": 2.8,      # ~168 wpm → 19-20 words per 7 sec
    "French": 2.5,       # ~150 wpm → 17-18 words per 7 sec
    "German": 2.2,       # ~132 wpm → 15-16 words per 7 sec
    "Portuguese": 2.6,   # ~156 wpm → 18-19 words per 7 sec
    "Dutch": 2.3,        # ~138 wpm → 16-17 words per 7 sec
    "Polish": 2.4,       # ~144 wpm → 17 words per 7 sec
    "Russian": 2.3,      # ~138 wpm → 16-17 words per 7 sec
    "Japanese": 4.0,     # ~240 morae/min → 28 chars per 7 sec
    "Chinese": 3.5,      # ~210 chars/min → 24-25 chars per 7 sec
    "Korean": 3.5,       # Similar to Chinese
    "Arabic": 2.5,       # ~150 wpm → 17-18 words per 7 sec
    "Hindi": 2.6,        # ~156 wpm → 18-19 words per 7 sec
}

TARGET_DURATION_SECONDS = 7

@app.post("/api/split-script")
async def split_script(request: ScriptSplitRequest):
    """
    Split a full script into ~7 second dialogue lines using OpenAI.
    Preserves the EXACT original text - only splits, never rewrites.
    Every line MUST be approximately 7 seconds (enforced via post-processing).
    """
    import os
    
    # Get OpenAI API key
    openai_key = os.environ.get("OPENAI_API_KEY")
    if not openai_key:
        raise HTTPException(status_code=400, detail="OpenAI API key not configured")
    
    # Get language-specific rate
    words_per_sec = LANGUAGE_SPEAKING_RATES.get(request.language, 2.5)
    target_words = int(words_per_sec * TARGET_DURATION_SECONDS)
    min_words = max(10, target_words - 5)  # Minimum words per line
    
    # Count total words to estimate expected clips
    total_words = len(request.script.split())
    expected_clips = max(1, round(total_words / target_words))
    
    try:
        from openai import OpenAI
        client = OpenAI(api_key=openai_key)
        
        prompt = f"""TASK: Split this script into chunks of EXACTLY ~{target_words} words each.

⚠️ ABSOLUTE REQUIREMENTS:
1. EVERY chunk MUST have AT LEAST {min_words} words (this is ~7 seconds of speech)
2. NEVER create a chunk with less than {min_words} words
3. If a sentence is short, COMBINE it with the next sentence(s) until you reach {min_words}+ words
4. The LAST chunk can be slightly shorter only if all remaining text is less than {min_words} words
5. Preserve EXACT original text - do NOT add, remove, or change any words

ORIGINAL SCRIPT ({total_words} total words):
"{request.script}"

MATH: {total_words} words ÷ {target_words} words = ~{expected_clips} chunks expected

EXAMPLES of what NOT to do:
❌ ["Short sentence.", "Another short one."] - BAD, each under {min_words} words
✅ ["Short sentence. Another short one. And more text here."] - GOOD, combined to reach {min_words}+ words

OUTPUT: JSON array only. Each string MUST have {min_words}+ words.
["chunk with {min_words}+ words here", "another chunk with {min_words}+ words"]"""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=4000
        )
        
        result = response.choices[0].message.content.strip()
        
        # Parse JSON - handle potential markdown code blocks
        if result.startswith("```"):
            result = result.split("```")[1]
            if result.startswith("json"):
                result = result[4:]
            result = result.strip()
        
        lines = json.loads(result)
        
        if not isinstance(lines, list) or len(lines) == 0:
            raise ValueError("Invalid response format")
        
        # POST-PROCESSING: Merge any lines that are too short
        merged_lines = []
        buffer = ""
        
        for line in lines:
            if buffer:
                buffer += " " + line.strip()
            else:
                buffer = line.strip()
            
            word_count = len(buffer.split())
            
            # If buffer has enough words, add it to merged_lines
            if word_count >= min_words:
                merged_lines.append(buffer)
                buffer = ""
        
        # Handle remaining buffer
        if buffer:
            if merged_lines:
                # Append to last line if buffer is too short
                buffer_words = len(buffer.split())
                if buffer_words < min_words:
                    merged_lines[-1] = merged_lines[-1] + " " + buffer
                else:
                    merged_lines.append(buffer)
            else:
                # Only one line in total
                merged_lines.append(buffer)
        
        # Clean up whitespace
        merged_lines = [" ".join(line.split()) for line in merged_lines]
        
        # Calculate average duration estimate using language-specific rate
        total_words_result = sum(len(line.split()) for line in merged_lines)
        avg_words = total_words_result / len(merged_lines) if merged_lines else 0
        avg_duration = round(avg_words / words_per_sec, 1)
        
        # Calculate per-line stats
        line_stats = []
        for line in merged_lines:
            wc = len(line.split())
            dur = round(wc / words_per_sec, 1)
            line_stats.append({"words": wc, "duration_sec": dur})
        
        return {
            "success": True,
            "lines": merged_lines,
            "count": len(merged_lines),
            "avg_duration": avg_duration,
            "total_words": total_words_result,
            "target_words_per_line": target_words,
            "min_words_per_line": min_words,
            "language": request.language,
            "line_stats": line_stats
        }
        
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse AI response: {str(e)}")
    except ImportError:
        raise HTTPException(status_code=500, detail="OpenAI library not installed")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Script splitting failed: {str(e)}")


# ============ Error Codes Reference ============

@app.get("/api/error-codes")
async def get_error_codes():
    """Get list of all error codes and their meanings"""
    return {
        code.value: {
            "name": code.name,
            "value": code.value,
        }
        for code in ErrorCode
    }


# ============ Health Check ============

@app.api_route("/api/health", methods=["GET", "HEAD"])
async def health_check():
    """Health check endpoint (supports GET and HEAD for monitoring services)"""
    # Check if genai SDK is available
    try:
        from veo_generator import GENAI_AVAILABLE
        sdk_status = "installed" if GENAI_AVAILABLE else "not_installed"
    except:
        sdk_status = "unknown"
    
    # Check storage configuration
    try:
        from backends.storage import is_storage_configured, get_storage_status
        storage_status = get_storage_status()
    except Exception as e:
        storage_status = {"configured": False, "error": str(e)}
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "workers": {
            "running_jobs": len(worker.running_jobs),
            "max_workers": worker.max_workers,
        },
        "sdk": {
            "google_genai": sdk_status,
            "message": "Video generation available" if sdk_status == "installed" else "Install google-genai for video generation"
        },
        "storage": storage_status
    }


@app.get("/api/storage-status")
async def get_storage_status_endpoint():
    """
    Check object storage (S3/R2) configuration status.
    
    Returns details about whether storage is configured and working.
    This is important for redo functionality - without storage, 
    redos will fail if the server restarts.
    """
    from backends.storage import is_storage_configured, get_storage, get_storage_status
    
    status = get_storage_status()
    
    # Try a simple operation to verify connectivity
    if status["configured"]:
        try:
            storage = get_storage()
            # Just try to list objects with a limit of 1 to verify connection
            storage.client.list_objects_v2(
                Bucket=storage.bucket_name,
                MaxKeys=1
            )
            status["connection"] = "ok"
        except Exception as e:
            status["connection"] = "failed"
            status["connection_error"] = str(e)
    else:
        status["connection"] = "not_configured"
        status["setup_hint"] = "Set S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET environment variables"
    
    return status


# ============ Admin - API Keys ============

@app.get("/api/admin/keys")
async def get_api_keys_status():
    """
    Check status of API keys configured on the server.
    
    Keys are loaded from .env file:
    - GEMINI_API_KEY_1, GEMINI_API_KEY_2, etc.
    - OPENAI_API_KEY (optional)
    """
    from config import key_pool
    
    status = api_keys_config.get_status()
    pool_status = key_pool.get_status()
    
    # Add masked preview of keys with block status
    masked_keys = []
    for i, key in enumerate(api_keys_config.gemini_api_keys):
        if len(key) > 12:
            masked = f"{key[:8]}...{key[-4:]}"
        else:
            masked = "***"
        
        is_blocked = api_keys_config.is_key_blocked(i)
        blocked_info = None
        if is_blocked and i in api_keys_config.blocked_keys:
            from datetime import datetime, timedelta
            block_time = api_keys_config.blocked_keys[i]
            unblock_time = block_time + timedelta(hours=api_keys_config.block_duration_hours)
            remaining = unblock_time - datetime.now()
            blocked_info = {
                "blocked_at": block_time.isoformat(),
                "unblocks_at": unblock_time.isoformat(),
                "remaining_hours": round(max(0, remaining.total_seconds() / 3600), 1)
            }
        
        # Check if reserved by a job
        reserved_by = pool_status["reservations"].get(i, None)
        
        masked_keys.append({
            "index": i + 1,
            "masked": masked,
            "is_current": i == api_keys_config.current_key_index,
            "is_blocked": is_blocked,
            "blocked_info": blocked_info,
            "reserved_by": reserved_by[:8] if reserved_by else None,
        })
    
    return {
        **status,
        "pool_status": pool_status,
        "gemini_keys": masked_keys,
        "openai_masked": f"{api_keys_config.openai_api_key[:8]}...{api_keys_config.openai_api_key[-4:]}" if api_keys_config.openai_api_key else None,
        "config_file": ".env",
        "block_duration_hours": api_keys_config.block_duration_hours,
        "instructions": "Add keys to .env file and restart server to update"
    }


@app.post("/api/admin/keys/unblock/{key_index}")
async def unblock_api_key(key_index: int):
    """
    Manually unblock a specific API key before the 12h timeout.
    key_index is 1-based (1, 2, 3, etc.)
    """
    actual_index = key_index - 1  # Convert to 0-based
    
    if actual_index < 0 or actual_index >= len(api_keys_config.gemini_api_keys):
        raise HTTPException(status_code=400, detail=f"Invalid key index. Must be 1-{len(api_keys_config.gemini_api_keys)}")
    
    if actual_index not in api_keys_config.blocked_keys:
        return {
            "success": True,
            "message": f"Key {key_index} was not blocked",
            "key_index": key_index
        }
    
    del api_keys_config.blocked_keys[actual_index]
    api_keys_config._save_blocked_keys()  # Persist to disk
    
    return {
        "success": True,
        "message": f"Key {key_index} has been unblocked",
        "key_index": key_index,
        "available_keys": api_keys_config.get_available_key_count(),
        "blocked_keys": len(api_keys_config.blocked_keys)
    }


@app.post("/api/admin/keys/unblock-all")
async def unblock_all_api_keys():
    """
    Unblock all API keys at once.
    """
    blocked_count = len(api_keys_config.blocked_keys)
    api_keys_config.blocked_keys.clear()
    api_keys_config._save_blocked_keys()  # Persist to disk
    
    return {
        "success": True,
        "message": f"Unblocked {blocked_count} keys",
        "unblocked_count": blocked_count,
        "available_keys": api_keys_config.get_available_key_count()
    }


@app.post("/api/admin/keys/rotate")
async def rotate_api_key(block_current: bool = False):
    """Manually rotate to the next Gemini API key"""
    if not api_keys_config.gemini_api_keys:
        raise HTTPException(status_code=400, detail="No Gemini keys configured")
    
    old_index = api_keys_config.current_key_index
    api_keys_config.rotate_key(block_current=block_current)
    new_index = api_keys_config.current_key_index
    
    return {
        "success": True,
        "previous_index": old_index,
        "current_index": new_index,
        "total_keys": len(api_keys_config.gemini_api_keys)
    }


@app.post("/api/admin/keys/reload")
async def reload_api_keys():
    """
    Reload API keys from .env file without restarting server.
    Useful after updating .env file.
    """
    from config import get_gemini_keys_from_env, get_openai_key_from_env
    from dotenv import load_dotenv
    
    # Reload .env file
    load_dotenv(override=True)
    
    # Update keys
    old_count = len(api_keys_config.gemini_api_keys)
    api_keys_config.gemini_api_keys = get_gemini_keys_from_env()
    api_keys_config.openai_api_key = get_openai_key_from_env()
    api_keys_config.current_key_index = 0  # Reset to first key
    
    new_count = len(api_keys_config.gemini_api_keys)
    
    return {
        "success": True,
        "previous_gemini_count": old_count,
        "current_gemini_count": new_count,
        "openai_configured": api_keys_config.openai_api_key is not None,
        "message": f"Loaded {new_count} Gemini key(s) from .env"
    }


class ValidateKeyRequest(BaseModel):
    api_key: str


@app.post("/api/admin/keys/validate")
async def validate_gemini_key(request: ValidateKeyRequest):
    """
    Validate a Gemini API key by making a test API call.
    Returns whether the key is valid, quota status, and any errors.
    """
    import httpx
    
    api_key = request.api_key.strip()
    
    if not api_key:
        return {
            "valid": False,
            "error": "No API key provided",
            "details": None
        }
    
    # Mask key for logging
    masked_key = api_key[:8] + "..." + api_key[-4:] if len(api_key) > 12 else "***"
    
    try:
        # Test with a simple models list request (doesn't consume quota)
        async with httpx.AsyncClient(timeout=30.0) as client:
            # First, try listing models (free, no quota)
            list_url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
            response = await client.get(list_url)
            
            if response.status_code == 200:
                models_data = response.json()
                models = models_data.get("models", [])
                
                # Check for Veo model specifically
                veo_available = any("veo" in m.get("name", "").lower() for m in models)
                gemini_available = any("gemini" in m.get("name", "").lower() for m in models)
                
                # Try a minimal generateContent request to check quota
                # Using gemini-2.0-flash which is fast and cheap
                generate_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
                test_payload = {
                    "contents": [{"parts": [{"text": "Say 'OK'"}]}],
                    "generationConfig": {"maxOutputTokens": 5}
                }
                
                gen_response = await client.post(generate_url, json=test_payload)
                
                quota_ok = gen_response.status_code == 200
                quota_error = None
                
                if not quota_ok:
                    error_data = gen_response.json() if gen_response.content else {}
                    quota_error = error_data.get("error", {}).get("message", f"Status {gen_response.status_code}")
                
                return {
                    "valid": True,
                    "key_preview": masked_key,
                    "models_accessible": len(models),
                    "veo_available": veo_available,
                    "gemini_available": gemini_available,
                    "quota_ok": quota_ok,
                    "quota_error": quota_error,
                    "message": "✅ Key is valid" + (" and has quota" if quota_ok else " but quota may be exhausted")
                }
            
            elif response.status_code == 400:
                return {
                    "valid": False,
                    "key_preview": masked_key,
                    "error": "Invalid API key format",
                    "details": response.json() if response.content else None
                }
            
            elif response.status_code == 403:
                error_data = response.json() if response.content else {}
                error_msg = error_data.get("error", {}).get("message", "Access denied")
                return {
                    "valid": False,
                    "key_preview": masked_key,
                    "error": f"API key not authorized: {error_msg}",
                    "details": error_data
                }
            
            elif response.status_code == 429:
                return {
                    "valid": True,
                    "key_preview": masked_key,
                    "quota_ok": False,
                    "error": "Rate limited - key is valid but quota exhausted",
                    "message": "⚠️ Key is valid but currently rate limited"
                }
            
            else:
                return {
                    "valid": False,
                    "key_preview": masked_key,
                    "error": f"Unexpected response: {response.status_code}",
                    "details": response.text[:500] if response.text else None
                }
                
    except httpx.TimeoutException:
        return {
            "valid": None,
            "key_preview": masked_key,
            "error": "Request timed out - could not verify key",
            "message": "⚠️ Could not verify key (timeout)"
        }
    except Exception as e:
        return {
            "valid": None,
            "key_preview": masked_key,
            "error": f"Validation error: {str(e)}",
            "message": "⚠️ Could not verify key"
        }


# ============ Debug Clip Versions ============

@app.get("/api/debug/clip/{clip_id}/versions")
async def debug_clip_versions(
    clip_id: int,
    db: DBSession = Depends(get_db_session),
):
    """Debug endpoint to view raw versions_json for a clip"""
    clip = db.query(Clip).filter(Clip.id == clip_id).first()
    if not clip:
        raise HTTPException(status_code=404, detail="Clip not found")
    
    versions = json.loads(clip.versions_json) if clip.versions_json else []
    return {
        "clip_id": clip.id,
        "clip_index": clip.clip_index,
        "status": clip.status,
        "output_filename": clip.output_filename,
        "generation_attempt": clip.generation_attempt,
        "versions_json_raw": clip.versions_json,
        "versions_count": len(versions),
        "versions": versions
    }


# ============ Debug Screenshots ============

@app.get("/api/debug/screenshots")
async def list_debug_screenshots(
    limit: int = 50,
    current_user: User = Depends(get_current_user)
):
    """
    List debug screenshots stored in R2.
    Returns list of screenshots with presigned URLs.
    """
    from backends.storage import is_storage_configured, get_storage
    
    if not is_storage_configured():
        raise HTTPException(status_code=503, detail="Storage not configured")
    
    try:
        storage = get_storage()
        
        # List screenshots in debug folder
        keys = storage.list_objects(prefix="debug/screenshots/", max_keys=limit)
        
        # Generate presigned URLs for each (valid for 1 hour)
        screenshots = []
        for key in reversed(keys):  # Newest first (by filename which has timestamp)
            filename = key.split("/")[-1]
            url = storage.get_presigned_url(key, expires_in=3600)
            
            # Parse timestamp from filename (format: YYYYMMDD_HHMMSS_name.png)
            parts = filename.replace(".png", "").split("_")
            if len(parts) >= 3:
                date_str = parts[0]
                time_str = parts[1]
                name = "_".join(parts[2:])
                try:
                    timestamp = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]} {time_str[:2]}:{time_str[2:4]}:{time_str[4:]}"
                except Exception:
                    timestamp = "unknown"
            else:
                name = filename
                timestamp = "unknown"
            
            screenshots.append({
                "key": key,
                "filename": filename,
                "name": name,
                "timestamp": timestamp,
                "url": url
            })
        
        return {
            "count": len(screenshots),
            "screenshots": screenshots
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list screenshots: {str(e)}")


@app.get("/debug/screenshots", response_class=HTMLResponse)
async def debug_screenshots_gallery(
    request: Request,
    limit: int = 100,
    current_user: User = Depends(get_current_user)
):
    """
    HTML gallery page for viewing debug screenshots.
    """
    from backends.storage import is_storage_configured, get_storage
    
    if not is_storage_configured():
        return HTMLResponse("<h1>Storage not configured</h1>")
    
    try:
        storage = get_storage()
        keys = storage.list_objects(prefix="debug/screenshots/", max_keys=limit)
        
        # Build screenshots list (newest first)
        screenshots = []
        for key in reversed(keys):
            filename = key.split("/")[-1]
            url = storage.get_presigned_url(key, expires_in=3600)
            
            parts = filename.replace(".png", "").split("_")
            if len(parts) >= 3:
                date_str = parts[0]
                time_str = parts[1]
                name = "_".join(parts[2:])
                try:
                    timestamp = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]} {time_str[:2]}:{time_str[2:4]}:{time_str[4:]}"
                except Exception:
                    timestamp = "unknown"
            else:
                name = filename
                timestamp = "unknown"
            
            screenshots.append({
                "name": name,
                "timestamp": timestamp,
                "url": url
            })
        
        # Generate HTML
        html = """
<!DOCTYPE html>
<html>
<head>
    <title>Flow Debug Screenshots</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a2e;
            color: #eee;
            margin: 0;
            padding: 20px;
        }
        h1 {
            color: #00d4ff;
            margin-bottom: 20px;
        }
        .info {
            color: #888;
            margin-bottom: 20px;
        }
        .gallery {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
            gap: 20px;
        }
        .screenshot {
            background: #16213e;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        }
        .screenshot img {
            width: 100%;
            height: auto;
            display: block;
            cursor: pointer;
        }
        .screenshot img:hover {
            opacity: 0.9;
        }
        .screenshot .info-bar {
            padding: 10px;
            background: #0f3460;
        }
        .screenshot .name {
            font-weight: bold;
            color: #00d4ff;
        }
        .screenshot .timestamp {
            font-size: 12px;
            color: #888;
        }
        .modal {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.9);
            z-index: 1000;
            cursor: pointer;
        }
        .modal img {
            max-width: 95%;
            max-height: 95%;
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
        }
        .back-link {
            display: inline-block;
            margin-bottom: 20px;
            color: #00d4ff;
            text-decoration: none;
        }
        .back-link:hover {
            text-decoration: underline;
        }
    </style>
</head>
<body>
    <a href="/" class="back-link">← Back to Studio</a>
    <h1>🔍 Flow Debug Screenshots</h1>
    <p class="info">Showing """ + str(len(screenshots)) + """ screenshots (newest first). Click to enlarge.</p>
    
    <div class="gallery">
"""
        
        for s in screenshots:
            html += f"""
        <div class="screenshot">
            <img src="{s['url']}" alt="{s['name']}" onclick="showModal(this.src)">
            <div class="info-bar">
                <div class="name">{s['name']}</div>
                <div class="timestamp">{s['timestamp']}</div>
            </div>
        </div>
"""
        
        html += """
    </div>
    
    <div class="modal" id="modal" onclick="hideModal()">
        <img id="modal-img" src="">
    </div>
    
    <script>
        function showModal(src) {
            document.getElementById('modal-img').src = src;
            document.getElementById('modal').style.display = 'block';
        }
        function hideModal() {
            document.getElementById('modal').style.display = 'none';
        }
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') hideModal();
        });
    </script>
</body>
</html>
"""
        return HTMLResponse(html)
        
    except Exception as e:
        return HTMLResponse(f"<h1>Error: {str(e)}</h1>")


@app.delete("/api/debug/screenshots")
async def delete_old_screenshots(
    older_than_hours: int = 24,
    current_user: User = Depends(get_current_user)
):
    """
    Delete debug screenshots older than specified hours.
    Helps clean up storage.
    """
    from backends.storage import is_storage_configured, get_storage
    from datetime import datetime, timedelta
    
    if not is_storage_configured():
        raise HTTPException(status_code=503, detail="Storage not configured")
    
    try:
        storage = get_storage()
        
        # List all screenshots
        keys = storage.list_objects(prefix="debug/screenshots/", max_keys=1000)
        
        cutoff = datetime.now() - timedelta(hours=older_than_hours)
        deleted = 0
        
        for key in keys:
            filename = key.split("/")[-1]
            # Parse timestamp from filename
            parts = filename.replace(".png", "").split("_")
            if len(parts) >= 2:
                try:
                    date_str = parts[0]
                    time_str = parts[1]
                    file_time = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
                    
                    if file_time < cutoff:
                        storage.client.delete_object(Bucket=storage.bucket_name, Key=key)
                        deleted += 1
                except Exception:
                    pass
        
        return {
            "deleted": deleted,
            "message": f"Deleted {deleted} screenshots older than {older_than_hours} hours"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete screenshots: {str(e)}")


# ============ Local Worker API ============
# These routes allow a local worker to fetch jobs and update status

LOCAL_WORKER_API_KEY = os.environ.get("LOCAL_WORKER_API_KEY", "local-worker-secret-key-12345")

def verify_local_worker_key(authorization: str = Header(None)):
    """Verify local worker API key"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing authorization header")
    
    token = authorization[7:]
    if token != LOCAL_WORKER_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True


@app.get("/api/local-worker/health")
async def local_worker_health():
    """Health check for local worker"""
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/api/local-worker/jobs/pending")
async def local_worker_get_pending_job(
    request: Request,
    worker_id: Optional[str] = Query(None, description="Worker ID for claiming"),
    exclude: Optional[str] = Query(None, description="Comma-separated job IDs to exclude (already being processed)"),
    db: DBSession = Depends(get_db_session),
    authorized: bool = Depends(verify_local_worker_key)
):
    """Get next pending Flow job with all clips.
    
    If worker_id is provided, atomically claims the job for that worker.
    Jobs claimed more than 10 minutes ago without completion are released.
    Pass exclude=id1,id2,... to skip jobs already being processed by this worker.
    """
    from sqlalchemy import or_, and_
    
    # Parse exclude list
    exclude_ids = [eid.strip() for eid in exclude.split(",") if eid.strip()] if exclude else []
    
    # Release stale claims (claimed > 10 minutes ago and not started)
    claim_timeout = datetime.utcnow() - timedelta(minutes=10)
    stale_jobs = db.query(Job).filter(
        Job.backend == 'flow',
        Job.status.in_(['pending', 'queued_for_flow']),
        Job.claimed_by_worker.isnot(None),
        Job.claimed_at < claim_timeout
    ).all()
    
    for stale_job in stale_jobs:
        print(f"[Worker] Releasing stale claim on job {stale_job.id[:8]} (was claimed by {stale_job.claimed_by_worker})", flush=True)
        stale_job.claimed_by_worker = None
        stale_job.claimed_at = None
    
    if stale_jobs:
        db.commit()
    
    # Find users who have an active personal worker (last_seen within 2 minutes)
    # VPS worker should NOT steal jobs from users running their own worker
    active_worker_cutoff = datetime.utcnow() - timedelta(minutes=2)
    active_user_ids = [
        row.user_id for row in db.query(UserWorkerToken.user_id).filter(
            UserWorkerToken.is_active == True,
            UserWorkerToken.last_seen >= active_worker_cutoff
        ).all()
    ]
    if active_user_ids:
        print(f"[Worker] Skipping jobs for {len(active_user_ids)} user(s) with active personal worker", flush=True)

    # Build query for available jobs
    # Either: unclaimed, OR claimed by this same worker
    # Exclude any jobs the worker is already processing
    if worker_id:
        query = db.query(Job).filter(
            Job.backend == 'flow',
            Job.status.in_(['pending', 'queued_for_flow']),
            or_(
                Job.claimed_by_worker.is_(None),
                Job.claimed_by_worker == worker_id
            )
        )

        # Skip jobs owned by users with their own active worker
        if active_user_ids:
            query = query.filter(Job.user_id.notin_(active_user_ids))

        # Exclude jobs already being processed
        if exclude_ids:
            query = query.filter(Job.id.notin_(exclude_ids))
        
        job = query.order_by(Job.created_at.asc()).first()
        
        if job and job.claimed_by_worker != worker_id:
            # Claim it
            job.claimed_by_worker = worker_id
            job.claimed_at = datetime.utcnow()
            db.commit()
            print(f"[Worker] Job {job.id[:8]} claimed by {worker_id}", flush=True)
    else:
        # No worker_id - just get unclaimed (legacy behavior)
        query = db.query(Job).filter(
            Job.backend == 'flow',
            Job.status.in_(['pending', 'queued_for_flow']),
            Job.claimed_by_worker.is_(None)
        )

        # Skip jobs owned by users with their own active worker
        if active_user_ids:
            query = query.filter(Job.user_id.notin_(active_user_ids))

        if exclude_ids:
            query = query.filter(Job.id.notin_(exclude_ids))
        
        job = query.order_by(Job.created_at.asc()).first()
    
    # v455: piggyback abort signals on this poll. The worker polls /pending
    # constantly (every 5-8s), so including the abort list here avoids a
    # dedicated heartbeat channel. Only jobs currently claimed by *this*
    # worker and marked abort_requested appear — other workers' aborts
    # aren't this worker's concern.
    aborted_jobs = []
    if worker_id:
        try:
            rows = db.query(Job.id).filter(
                Job.claimed_by_worker == worker_id,
                Job.abort_requested == True,  # noqa: E712
            ).all()
            aborted_jobs = [r.id for r in rows]
        except Exception as e:
            # If the column doesn't exist yet (pre-migration), don't crash —
            # just return an empty list
            print(f"[LocalWorker] abort_requested lookup failed (migration pending?): {e}", flush=True)
            aborted_jobs = []

    if not job:
        return {"job": None, "aborted_jobs": aborted_jobs}
    
    print(f"[LocalWorker] Found job {job.id[:8]}, querying clips...", flush=True)
    clips = db.query(Clip).filter(Clip.job_id == job.id).order_by(Clip.clip_index.asc()).all()
    print(f"[LocalWorker] Found {len(clips)} clips for job {job.id[:8]}", flush=True)
    
    # DEBUG: If no clips, check if they exist at all
    if not clips:
        total_clips_in_db = db.query(Clip).filter(Clip.job_id == job.id).count()
        print(f"[LocalWorker] DEBUG: Total clips in DB for this job: {total_clips_in_db}", flush=True)
        # Check job's total_clips field
        print(f"[LocalWorker] DEBUG: job.total_clips = {job.total_clips}", flush=True)
    
    # Parse config JSON
    config = json.loads(job.config_json) if job.config_json else {}
    use_interpolation = config.get("use_interpolation", True)
    
    # Build base URL for frame downloads (use proxy to avoid SSL issues on Windows)
    base_url = str(request.base_url).rstrip('/')
    
    # Determine if single image mode (all clips have same start_frame or only one unique frame)
    unique_frames = set(c.start_frame for c in clips if c.start_frame)
    single_image_mode = len(unique_frames) <= 1
    
    clips_data = []
    for clip in clips:
        # Get frame keys
        start_frame_key = clip.start_frame
        end_frame_key = clip.end_frame
        
        # Extract filename from R2 key (format: jobs/{job_id}/frames/{filename})
        start_filename = start_frame_key.split('/')[-1] if start_frame_key else None
        end_filename = end_frame_key.split('/')[-1] if end_frame_key else None
        
        clip_data = {
            "id": clip.id,
            "clip_index": clip.clip_index,
            "dialogue_text": clip.dialogue_text,
            "prompt": clip.prompt_text,  # Generated prompt
            "start_frame_key": start_frame_key,  # R2 key for frame
            "end_frame_key": end_frame_key,
            "status": clip.status,
            # Use proxy URLs instead of direct R2 presigned URLs
            "start_frame_url": f"{base_url}/api/local-worker/frames/{job.id}/{start_filename}" if start_filename else None,
            "end_frame_url": f"{base_url}/api/local-worker/frames/{job.id}/{end_filename}" if end_filename else None,
            # Storyboard/Scene mode fields for continue mode support
            "clip_mode": clip.clip_mode or "blend",
            "scene_index": clip.scene_index or 0,
        }
        
        clips_data.append(clip_data)
    
    print(f"[LocalWorker] Returning job {job.id[:8]} with {len(clips_data)} clips to worker", flush=True)
    
    return {
        "job": {
            "id": job.id,
            "aspect_ratio": config.get("aspect_ratio", "9:16"),
            "duration": config.get("duration", "8"),
            "language": config.get("language", "English"),
            "voice_profile": config.get("voice_profile", "") or config.get("user_context", ""),
            "resolution": config.get("resolution", "720p"),
            "use_interpolation": use_interpolation,
            "single_image_mode": single_image_mode,
            "flow_project_url": job.flow_project_url,
            "flow_variants_count": config.get("flow_variants_count", 2),
            "short_dialogue_mode": config.get("short_dialogue_mode", "optimized"),
            "prefix_short_enabled": config.get("prefix_short_enabled", False),
            "prefix_short_word": config.get("prefix_short_word", "only"),
            "prefix_short_threshold": config.get("prefix_short_threshold", 15),
            "clips": clips_data,
            "claimed_by": job.claimed_by_worker
        },
        "aborted_jobs": aborted_jobs,
    }


@app.get("/api/local-worker/clips/redo-pending")
async def local_worker_get_redo_clips(
    request: Request,
    worker_id: Optional[str] = Query(None, description="Worker ID for claiming"),
    db: DBSession = Depends(get_db_session),
    authorized: bool = Depends(verify_local_worker_key)
):
    """Get clips that need regeneration for Flow jobs.
    
    If worker_id is provided, atomically claims clips for that worker.
    Claims expire after 10 minutes if not completed.
    
    This ONLY handles Flow backend redos via 'flow_redo_queued' status.
    API backend redos use 'redo_queued' and are handled by the API worker.
    """
    from sqlalchemy import or_, and_
    
    # Release stale claims (claimed > 10 minutes ago)
    # NOTE: Now filtering for flow_redo_queued instead of redo_queued
    claim_timeout = datetime.utcnow() - timedelta(minutes=10)
    stale_clips = db.query(Clip).join(Job).filter(
        Job.backend == 'flow',
        Clip.status == ClipStatus.FLOW_REDO_QUEUED.value,  # Changed from 'redo_queued'
        Clip.claimed_by_worker.isnot(None),
        Clip.claimed_at < claim_timeout
    ).all()
    
    for stale_clip in stale_clips:
        # v468: zombie-loop prevention. If this clip has been sitting in
        # flow_redo_queued for more than 30 minutes (3+ claim/release
        # cycles) without ever completing, stop re-claiming. The worker
        # is clearly unable to process it — further re-claims just burn
        # worker time and lock the job. Mark it failed so the user sees
        # actionable buttons (Retry / Delete).
        _job = stale_clip.job
        _redo_started = stale_clip.claimed_at or stale_clip.updated_at or _job.updated_at
        ZOMBIE_THRESHOLD_MINUTES = 30
        if _redo_started and (datetime.utcnow() - _redo_started) > timedelta(minutes=ZOMBIE_THRESHOLD_MINUTES):
            print(f"[Worker] ⛔ Clip {stale_clip.id} has been stuck in flow_redo_queued for >{ZOMBIE_THRESHOLD_MINUTES}min — marking failed (zombie redo)", flush=True)
            stale_clip.status = ClipStatus.FAILED.value
            stale_clip.claimed_by_worker = None
            stale_clip.claimed_at = None
            stale_clip.approval_status = "pending_review"
            stale_clip.error_code = "REDO_ZOMBIE"
            stale_clip.error_message = f"Redo stuck: worker claimed this clip repeatedly over {ZOMBIE_THRESHOLD_MINUTES}+ minutes without completing. Click Retry to try again, or remove the clip."
            try:
                add_job_log(db, stale_clip.job_id,
                            f"Clip {stale_clip.clip_index + 1} marked failed — redo stuck in zombie loop for >{ZOMBIE_THRESHOLD_MINUTES}min",
                            "ERROR", "redo_zombie")
            except Exception:
                pass
            continue
        print(f"[Worker] Releasing stale claim on clip {stale_clip.id} (was claimed by {stale_clip.claimed_by_worker})", flush=True)
        stale_clip.claimed_by_worker = None
        stale_clip.claimed_at = None
    
    if stale_clips:
        db.commit()
    
    # Build query for available redo clips
    # IMPORTANT: Now using flow_redo_queued status for proper separation
    # Only return redo clips from recently active jobs (last 24h)
    #
    # v485: redo endpoint explicitly bumps job.updated_at on every redo
    # so the job satisfies the 24h window. v487: reverted the
    # `or_(..., Clip.updated_at >= redo_cutoff)` addition from v485 —
    # Clip has no updated_at column and referencing it caused every
    # poll to 500 with UndefinedColumn. Old orphaned redos are rescued
    # by the startup backfill in lifespan that bumps Job.updated_at on
    # any job containing flow_redo_queued clips.
    redo_cutoff = datetime.utcnow() - timedelta(hours=24)
    if worker_id:
        # Either: unclaimed, OR claimed by this same worker
        redo_clips = db.query(Clip).join(Job).filter(
            Job.backend == 'flow',
            Job.updated_at >= redo_cutoff,
            or_(
                # Normal Flow redo queue - unclaimed or claimed by this worker
                and_(
                    Clip.status == ClipStatus.FLOW_REDO_QUEUED.value,
                    or_(
                        Clip.claimed_by_worker.is_(None),
                        Clip.claimed_by_worker == worker_id
                    )
                ),
                # Failed Flow redos that API worker wrongly tried to process (legacy recovery)
                and_(
                    Clip.status == 'failed',
                    Clip.generation_attempt > 1,
                    Clip.error_message.ilike('%file not found%')
                )
            )
        ).order_by(Clip.id.asc()).all()
    else:
        # No worker_id - get unclaimed only (legacy behavior)
        redo_clips = db.query(Clip).join(Job).filter(
            Job.backend == 'flow',
            Job.updated_at >= redo_cutoff,
            or_(
                and_(
                    Clip.status == ClipStatus.FLOW_REDO_QUEUED.value,
                    Clip.claimed_by_worker.is_(None)
                ),
                and_(
                    Clip.status == 'failed',
                    Clip.generation_attempt > 1,
                    Clip.error_message.ilike('%file not found%')
                )
            )
        ).order_by(Clip.id.asc()).all()
    
    if not redo_clips:
        return {"clips": []}
    
    base_url = str(request.base_url).rstrip('/')
    
    clips_data = []
    for clip in redo_clips:
        job = clip.job
        
        # If this is a failed clip being recovered, reset its status to flow_redo_queued
        if clip.status == 'failed':
            print(f"[LocalWorker] Recovering Flow redo: clip {clip.id} (job {job.id[:8]})", flush=True)
            clip.status = ClipStatus.FLOW_REDO_QUEUED.value  # Changed from 'redo_queued'
            clip.error_message = None
            db.commit()
            # No job log - this is expected behavior, not worth cluttering logs
        
        # Claim clip if worker_id provided and not already claimed by this worker
        if worker_id and clip.claimed_by_worker != worker_id:
            clip.claimed_by_worker = worker_id
            clip.claimed_at = datetime.utcnow()
            # NOTE: Do NOT change status to 'generating' here. The worker changes
            # it when it actually starts processing (in process_redo_clip).
            # If we change it here and the worker drops the redo (account dead,
            # queue stuck, thread crashed), the clip is permanently lost — it's
            # no longer flow_redo_queued so it won't be re-polled, and
            # queued_redo_keys blocks re-queuing.
            # The 10-minute stale claim release handles recovery.
            db.commit()
            print(f"[Worker] Clip {clip.id} (redo) claimed by {worker_id}", flush=True)
            add_job_log(db, clip.job_id, f"Flow redo for clip {clip.clip_index + 1} claimed by local worker", "INFO", "redo")
        elif worker_id and clip.claimed_by_worker == worker_id:
            # Already claimed by this worker - skip (don't log again to avoid spam)
            pass
        
        # Get frame URLs — with fallback if clip has no frame keys
        start_frame_key = clip.start_frame
        end_frame_key = clip.end_frame
        
        if not start_frame_key and job.frames_storage_keys:
            try:
                frames_keys = json.loads(job.frames_storage_keys)
                dialogue_data = json.loads(job.dialogue_json) if job.dialogue_json else {}
                dialogue_lines = dialogue_data.get("lines", [])
                uploaded_frames = sorted(frames_keys.keys())
                num_images = len(uploaded_frames)
                
                if num_images > 0 and clip.clip_index < len(dialogue_lines):
                    line_data = dialogue_lines[clip.clip_index]
                    start_img_idx = line_data.get("start_image_idx", 0) if isinstance(line_data, dict) else 0
                    start_fname = uploaded_frames[start_img_idx % num_images]
                    start_frame_key = f"jobs/{job.id}/frames/{start_fname}"
                    clip_mode = clip.clip_mode or "blend"
                    if clip_mode == "blend":
                        end_frame_key = start_frame_key
                    clip.start_frame = start_frame_key
                    if end_frame_key:
                        clip.end_frame = end_frame_key
                    db.commit()
                    print(f"[LocalRedo] Backfilled frame keys for clip {clip.clip_index}: start={start_fname}", flush=True)
            except Exception as _fb_err:
                print(f"[LocalRedo] Frame key backfill failed: {_fb_err}", flush=True)
        
        start_filename = start_frame_key.split('/')[-1] if start_frame_key else None
        end_filename = end_frame_key.split('/')[-1] if end_frame_key else None
        
        # Get job config for voice_profile if available
        job_config = {}
        if job.config_json:
            try:
                import json
                job_config = json.loads(job.config_json) if isinstance(job.config_json, str) else job.config_json
            except:
                pass
        
        clips_data.append({
            "id": clip.id,
            "job_id": job.id,
            "clip_index": clip.clip_index,
            "dialogue_text": clip.dialogue_text,
            "prompt": clip.prompt_text,
            "language": job_config.get("language", "English"),
            "duration": job_config.get("duration", "8"),
            "voice_profile": job_config.get("voice_profile", "") or job_config.get("user_context", ""),
            "start_frame_url": f"{base_url}/api/local-worker/frames/{job.id}/{start_filename}" if start_filename else None,
            "end_frame_url": f"{base_url}/api/local-worker/frames/{job.id}/{end_filename}" if end_filename else None,
            "flow_project_url": job.flow_project_url,
            "generation_attempt": clip.generation_attempt,
            "redo_reason": clip.redo_reason,
            "claimed_by": clip.claimed_by_worker,
            # Storyboard/Scene mode fields for continue mode support
            "clip_mode": clip.clip_mode or "blend",
            "scene_index": clip.scene_index or 0,
            "short_dialogue_mode": job_config.get("short_dialogue_mode", "optimized"),
            "prefix_short_enabled": job_config.get("prefix_short_enabled", False),
            "prefix_short_word": job_config.get("prefix_short_word", "only"),
            "prefix_short_threshold": job_config.get("prefix_short_threshold", 15),
            "flow_variants_count": job_config.get("flow_variants_count", 2),
        })
    
    return {"clips": clips_data}


class LocalWorkerJobUpdate(BaseModel):
    status: Optional[str] = None
    error_message: Optional[str] = None
    flow_project_url: Optional[str] = None


@app.get("/api/local-worker/jobs/{job_id}")
async def local_worker_get_job(
    job_id: str,
    db: DBSession = Depends(get_db_session),
    authorized: bool = Depends(verify_local_worker_key)
):
    """Get job details including clip statuses — for worker dedup and status checks."""
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(404, "Job not found")
    clips = db.query(Clip).filter(Clip.job_id == job_id).order_by(Clip.clip_index.asc()).all()
    return {
        "id": job.id,
        "status": job.status,
        "total_clips": job.total_clips,
        "completed_clips": job.completed_clips,
        "clips": [{"id": c.id, "clip_index": c.clip_index, "status": c.status,
                   "output_filename": c.output_filename,
                   "output_url": c.output_url,
                   "approval_status": c.approval_status or "pending_review"}
                  for c in clips]
    }


@app.post("/api/local-worker/jobs/{job_id}/status")
async def local_worker_update_job_status(
    job_id: str,
    update: LocalWorkerJobUpdate,
    db: DBSession = Depends(get_db_session),
    authorized: bool = Depends(verify_local_worker_key)
):
    """Update job status"""
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if update.status:
        job.status = update.status
        # Clear claim when job is completed or failed
        if update.status in ['completed', 'failed', 'cancelled']:
            job.claimed_by_worker = None
            job.claimed_at = None
    if update.error_message:
        job.error_message = update.error_message
    if update.flow_project_url:
        job.flow_project_url = update.flow_project_url
    job.updated_at = datetime.utcnow()
    
    db.commit()
    return {"success": True, "job_id": job_id, "status": job.status}


class LocalWorkerClipUpdate(BaseModel):
    status: Optional[str] = None
    output_url: Optional[str] = None
    output_key: Optional[str] = None
    error_message: Optional[str] = None


@app.post("/api/local-worker/clips/{clip_id}/status")
async def local_worker_update_clip_status(
    clip_id: str,
    update: LocalWorkerClipUpdate,
    db: DBSession = Depends(get_db_session),
    authorized: bool = Depends(verify_local_worker_key)
):
    """Update clip status"""
    # Use FOR UPDATE to prevent race condition with upload endpoint
    clip = db.query(Clip).filter(Clip.id == clip_id).with_for_update().first()
    if not clip:
        raise HTTPException(status_code=404, detail="Clip not found")
    
    # Clean up images_dir for Flow jobs (frames are in R2, not local disk)
    # This fixes existing Flow jobs that still have local paths set
    job = db.query(Job).filter(Job.id == clip.job_id).first()
    if job and job.backend == 'flow' and job.images_dir:
        print(f"[LocalWorker] Cleaning up images_dir for Flow job {job.id[:8]}", flush=True)
        job.images_dir = ""  # Empty string instead of None (DB has NOT NULL constraint)
    
    old_status = clip.status
    
    if update.status:
        clip.status = update.status
        # Clear claim when clip is completed or failed
        if update.status in ['completed', 'failed']:
            clip.claimed_by_worker = None
            clip.claimed_at = None
        # v464: if a clip transitions back to a non-completed state
        # (worker reclaim, retry loop, redo queueing), its prior
        # "approved" label no longer reflects reality — the video
        # content is about to change. Reset approval so the UI label
        # and the review-status summary agree. Previously the UI would
        # keep showing "✓ Approved" for clips that were actually
        # regenerating, while the summary correctly counted them as
        # not-yet-approved, producing the "9 cards say approved but
        # only 4 OK in the banner" mismatch.
        if update.status != 'completed' and clip.approval_status == 'approved':
            clip.approval_status = 'pending_review'
        # When clip is re-queued for redo, reset job back to processing so UI shows correctly
        if update.status == 'flow_redo_queued':
            clip.claimed_by_worker = None
            clip.claimed_at = None
            _redo_job = db.query(Job).filter(Job.id == clip.job_id).first()
            if _redo_job and _redo_job.status == 'completed':
                _redo_job.status = 'processing'
                _redo_job.completed_at = None
                # Recalculate completed count excluding this clip
                _done = db.query(Clip).filter(
                    Clip.job_id == clip.job_id,
                    Clip.status == ClipStatus.COMPLETED.value
                ).count()
                _redo_job.completed_clips = _done
                if _redo_job.total_clips > 0:
                    _redo_job.progress_percent = int((_done / _redo_job.total_clips) * 100)
            
            # Log Flow redo failures for debugging
            if update.status == 'failed' and old_status == ClipStatus.GENERATING.value:
                error_msg = update.error_message or "Unknown error"
                add_job_log(
                    db, clip.job_id,
                    f"⚠️ Flow redo for clip {clip.clip_index + 1} failed: {error_msg[:100]}",
                    "ERROR", "redo"
                )
        # Clear error state when status is NOT failed (e.g., generating, completed)
        if update.status != 'failed':
            clip.error_message = None
            clip.error_code = None
    if update.output_url:
        clip.output_url = update.output_url
    if update.output_key:
        clip.output_key = update.output_key
    if update.error_message:
        clip.error_message = update.error_message
    
    # When completing a clip (from redo or initial generation), update approval status
    # Include flow_redo_queued for Flow backend redos
    if update.status == 'completed' and old_status in ['generating', 'redo_queued', 'flow_redo_queued']:
        clip.approval_status = 'pending_review'
        clip.completed_at = datetime.utcnow()
        
        # Extract filename from output_url for video playback
        if update.output_url:
            # URL format: .../outputs/clip_X.mp4
            import re
            match = re.search(r'/outputs/([^/]+\.mp4)', update.output_url)
            if match:
                clip.output_filename = match.group(1)
        
        # Set selected_variant based on actual versions count (already populated by upload endpoint)
        versions = json.loads(clip.versions_json) if clip.versions_json else []
        if versions:
            # Default to variant 1 (first) on completion — user can browse others with ◀▶
            # output_filename is already set to variant 1 by the upload endpoint
            clip.selected_variant = 1
        
        # Log completion
        add_job_log(
            db, clip.job_id,
            f"Clip {clip.clip_index + 1} completed via Flow backend (all variants uploaded)",
            "INFO", "flow"
        )
        
        # Update job's completed_clips counter
        job = db.query(Job).filter(Job.id == clip.job_id).first()
        if job:
            completed = db.query(Clip).filter(
                Clip.job_id == clip.job_id,
                Clip.status == ClipStatus.COMPLETED.value
            ).count()
            job.completed_clips = completed
            if job.total_clips > 0:
                job.progress_percent = int((completed / job.total_clips) * 100)
            # Check if all clips are completed
            if completed >= job.total_clips:
                job.status = "completed"
                job.completed_at = datetime.utcnow()
    
    clip.updated_at = datetime.utcnow()
    
    db.commit()
    return {"success": True, "clip_id": clip_id, "status": clip.status}


@app.get("/api/local-worker/frames/{job_id}/{filename}")
async def local_worker_download_frame(
    job_id: str,
    filename: str,
    authorized: bool = Depends(verify_local_worker_key)
):
    """
    Download a frame for local worker.
    Proxies from R2 to avoid SSL issues on Windows.
    """
    from fastapi.responses import Response
    from backends.storage import is_storage_configured, get_storage
    
    # Build R2 key
    r2_key = f"jobs/{job_id}/frames/{filename}"
    
    # Check local filesystem first
    local_path = app_config.uploads_dir / job_id / filename
    if local_path.exists():
        with open(local_path, 'rb') as f:
            content = f.read()
        media_type = 'image/png' if filename.endswith('.png') else 'image/jpeg'
        return Response(content=content, media_type=media_type)
    
    # Download from R2
    if not is_storage_configured():
        raise HTTPException(status_code=503, detail="Storage not configured")
    
    try:
        storage = get_storage()
        
        # Download to temp file
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmp:
            tmp_path = tmp.name
        
        storage.download_file(r2_key, tmp_path)
        
        with open(tmp_path, 'rb') as f:
            content = f.read()
        
        # Clean up
        os.remove(tmp_path)
        
        media_type = 'image/png' if filename.endswith('.png') else 'image/jpeg'
        return Response(content=content, media_type=media_type)
        
    except Exception as e:
        print(f"[LocalWorker] Frame download error: {e}", flush=True)
        raise HTTPException(status_code=404, detail=f"Frame not found: {filename}")


class EnhanceFrameRequest(BaseModel):
    """Request body for frame enhancement"""
    frame_base64: str  # Base64 encoded extracted frame
    original_frame_key: Optional[str] = None  # R2 key of original scene image for facial consistency
    job_id: str  # Job ID for context and storage


@app.post("/api/local-worker/enhance-frame")
async def local_worker_enhance_frame(
    request: EnhanceFrameRequest,
    authorized: bool = Depends(verify_local_worker_key)
):
    """
    Enhance an extracted video frame using Nano Banana Pro (Gemini 3 Pro Image).
    
    This endpoint:
    1. Decodes the base64 frame
    2. Optionally downloads the original scene image from R2 for facial consistency
    3. Calls Gemini 3 Pro Image to upscale and fix facial features
    4. Returns the enhanced frame as base64
    
    If no Gemini keys are available or enhancement fails, returns the original frame.
    """
    import base64
    import tempfile
    from pathlib import Path
    from backends.storage import is_storage_configured, get_storage
    
    try:
        # Decode the input frame
        try:
            frame_bytes = base64.b64decode(request.frame_base64)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid base64 frame data: {e}")
        
        # Save to temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp:
            tmp.write(frame_bytes)
            frame_path = Path(tmp.name)
        
        # Try to get original scene image for facial consistency
        original_scene_path = None
        if request.original_frame_key and is_storage_configured():
            try:
                storage = get_storage()
                with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as orig_tmp:
                    original_scene_path = Path(orig_tmp.name)
                storage.download_file(request.original_frame_key, str(original_scene_path))
                print(f"[EnhanceFrame] Downloaded original scene image: {request.original_frame_key}", flush=True)
            except Exception as e:
                print(f"[EnhanceFrame] Could not download original scene image: {e}", flush=True)
                original_scene_path = None
        
        # Try to enhance with Nano Banana Pro
        enhanced_path = _enhance_frame_with_nano_banana(frame_path, original_scene_path)
        
        # Log to job if job_id provided
        job_id = getattr(request, 'job_id', '') or ''
        if job_id:
            try:
                with get_db() as db:
                    if enhanced_path:
                        add_job_log(db, job_id, f"🖼️ Frame enhanced via Nano Banana (continue mode)", "INFO", "system")
                    else:
                        add_job_log(db, job_id, f"⚠️ Frame enhancement unavailable — using raw extracted frame (continue mode)", "WARNING", "system")
                    db.commit()
            except Exception:
                pass
        
        # Read the result (enhanced or original if enhancement failed)
        with open(enhanced_path or frame_path, 'rb') as f:
            result_bytes = f.read()
        
        # Clean up temp files
        try:
            frame_path.unlink()
            if original_scene_path and original_scene_path.exists():
                original_scene_path.unlink()
            if enhanced_path and enhanced_path != frame_path and enhanced_path.exists():
                enhanced_path.unlink()
        except:
            pass
        
        # Return as base64
        result_base64 = base64.b64encode(result_bytes).decode('utf-8')
        
        return {
            "success": True,
            "enhanced": enhanced_path is not None,
            "frame_base64": result_base64
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[EnhanceFrame] Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        # Return original frame on error
        return {
            "success": False,
            "enhanced": False,
            "frame_base64": request.frame_base64,
            "error": str(e)
        }


def _enhance_frame_with_nano_banana(frame_path: Path, original_scene_image: Optional[Path] = None) -> Optional[Path]:
    """
    Enhance an extracted frame using Nano Banana 2 (gemini-3.1-flash-image-preview).
    Upscales and improves quality of the image.
    
    If original_scene_image is provided, also corrects facial features to match
    the original person (fixes AI drift in facial appearance).
    
    Returns path to enhanced frame, or None if enhancement failed/unavailable.
    """
    try:
        import google.genai as genai
        from google.genai import types
    except ImportError:
        print("[EnhanceFrame] google-genai not installed, skipping enhancement", flush=True)
        return None
    
    try:
        # Get Gemini API keys - check multiple sources
        api_key = None
        key_source = None
        
        # 1) Dedicated NANO_BANANA_API_KEY (highest priority)
        nano_key = os.environ.get("NANO_BANANA_API_KEY", "").strip()
        if nano_key:
            api_key = nano_key
            key_source = "NANO_BANANA_API_KEY"
        
        # 2) GEMINI_API_KEYS comma-separated list
        if not api_key:
            gemini_keys_str = os.environ.get("GEMINI_API_KEYS", "")
            if gemini_keys_str:
                gemini_keys = [k.strip() for k in gemini_keys_str.split(",") if k.strip()]
                if gemini_keys:
                    api_key = gemini_keys[0]
                    key_source = "GEMINI_API_KEYS"
        
        # 3) Individual GEMINI_API_KEY_N keys
        if not api_key:
            for i in range(1, 20):
                k = os.environ.get(f"GEMINI_API_KEY_{i}", "").strip()
                if k:
                    api_key = k
                    key_source = f"GEMINI_API_KEY_{i}"
                    break
        
        if not api_key:
            print("[EnhanceFrame] ❌ No API key available. Set NANO_BANANA_API_KEY or GEMINI_API_KEYS env var.", flush=True)
            return None
        
        print(f"[EnhanceFrame] Using key from {key_source} (ending ...{api_key[-6:]})", flush=True)
        client = genai.Client(api_key=api_key)
        
        # Read the extracted frame
        with open(frame_path, 'rb') as f:
            frame_bytes = f.read()
        
        # Determine mime type
        suffix = frame_path.suffix.lower()
        mime_type = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.webp': 'image/webp'
        }.get(suffix, 'image/jpeg')
        
        print(f"[EnhanceFrame] Enhancing frame with Nano Banana 2: {frame_path.name} ({len(frame_bytes)} bytes)", flush=True)
        
        # Build the prompt parts
        parts = [
            types.Part.from_bytes(data=frame_bytes, mime_type=mime_type),
        ]
        
        prompt_text = (
            "Enhance this photo by improving overall image quality while preserving the original subject, composition, and realism. "
            "Upscale the image to a higher resolution with sharp details and clear textures. "
            "Reduce blur, noise, grain, pixelation, and compression artifacts. "
            "Improve focus, lighting, contrast, color balance, skin tones, and dynamic range. "
            "Restore fine details in the face, hair, eyes, clothing, and background where possible without making the image look artificial. "
            "Keep the result natural, clean, and photorealistic. "
            "Do not change the person's identity, facial features, pose, expression, or background structure. "
            "Avoid over-smoothing, oversharpening, cartoon effects, or unrealistic edits. "
            "Produce a crisp, high-quality professional-looking enhanced version of the original image. "
            "Output only the enhanced image, no text."
        )
        
        parts.append(types.Part.from_text(text=prompt_text))
        
        contents = [
            types.Content(
                role="user",
                parts=parts
            )
        ]
        
        # Nano Banana 2 config — matches Google AI Studio reference
        config = types.GenerateContentConfig(
            response_modalities=["IMAGE", "TEXT"],
            image_config=types.ImageConfig(
                image_size="1K",
            ),
        )
        
        # Call with retry logic for overload
        max_retries = 3
        response = None
        for attempt in range(max_retries):
            try:
                print(f"[EnhanceFrame] Calling gemini-3.1-flash-image-preview (attempt {attempt + 1}/{max_retries})...", flush=True)
                response = client.models.generate_content(
                    model="gemini-3.1-flash-image-preview",
                    contents=contents,
                    config=config
                )
                break
            except Exception as e:
                error_str = str(e)
                print(f"[EnhanceFrame] API error (attempt {attempt + 1}): {error_str[:200]}", flush=True)
                if "overloaded" in error_str.lower() or "503" in error_str or "429" in error_str:
                    import time
                    wait_time = (attempt + 1) * 5
                    print(f"[EnhanceFrame] Retrying in {wait_time}s...", flush=True)
                    time.sleep(wait_time)
                    if attempt == max_retries - 1:
                        print(f"[EnhanceFrame] ❌ Still overloaded after {max_retries} attempts", flush=True)
                        return None
                else:
                    print(f"[EnhanceFrame] ❌ Non-retryable error: {error_str[:300]}", flush=True)
                    return None
        
        if not response:
            print("[EnhanceFrame] ❌ No response received", flush=True)
            return None
        
        if not response.candidates:
            block_reason = getattr(response, 'prompt_feedback', None)
            print(f"[EnhanceFrame] ❌ No candidates. Block reason: {block_reason}", flush=True)
            return None
        
        # Extract the image from response
        for part in response.candidates[0].content.parts:
            if hasattr(part, 'inline_data') and part.inline_data and part.inline_data.data:
                enhanced_path = frame_path.parent / f"{frame_path.stem}_enhanced.png"
                
                import base64
                image_data = part.inline_data.data
                if isinstance(image_data, str):
                    image_data = base64.b64decode(image_data)
                
                with open(enhanced_path, 'wb') as f:
                    f.write(image_data)
                
                print(f"[EnhanceFrame] ✓ Enhanced frame saved: {enhanced_path.name} ({len(image_data)} bytes)", flush=True)
                return enhanced_path
        
        # Log what we got instead
        part_types = [type(p).__name__ for p in response.candidates[0].content.parts]
        print(f"[EnhanceFrame] ❌ No image in response. Got parts: {part_types}", flush=True)
        for p in response.candidates[0].content.parts:
            if hasattr(p, 'text') and p.text:
                print(f"[EnhanceFrame] Model text: {p.text[:200]}", flush=True)
        return None
        
    except Exception as e:
        print(f"[EnhanceFrame] ❌ Enhancement error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return None


@app.get("/api/local-worker/clips/{clip_id}/approval-status")
async def local_worker_get_clip_approval_status(
    clip_id: int,
    db: DBSession = Depends(get_db_session),
    authorized: bool = Depends(verify_local_worker_key)
):
    """
    Get clip approval status for continue mode processing.
    
    Returns:
        - approval_status: 'pending_review', 'approved', 'rejected', 'max_attempts'
        - selected_variant: Which variant the user selected (1-based)
        - output_url: URL of the selected variant's video (for downloading)
        - has_video: Whether any video has been generated
    """
    clip = db.query(Clip).filter(Clip.id == clip_id).first()
    if not clip:
        raise HTTPException(status_code=404, detail="Clip not found")
    
    # Get the job for video URL construction
    job = db.query(Job).filter(Job.id == clip.job_id).first()
    
    # Parse versions to find the selected variant's video
    versions = []
    if clip.versions_json:
        try:
            versions = json.loads(clip.versions_json) if isinstance(clip.versions_json, str) else clip.versions_json
        except:
            versions = []
    
    selected_variant = clip.selected_variant or 1
    selected_video_url = None
    
    # Find the video URL for the selected variant
    for v in versions:
        if v.get('attempt') == 1 and v.get('variant') == selected_variant:
            selected_video_url = v.get('url')
            break
        # Fallback: if no variant field, use the attempt number
        if v.get('attempt') == selected_variant and not v.get('variant'):
            selected_video_url = v.get('url')
            break
    
    return {
        "clip_id": clip.id,
        "clip_index": clip.clip_index,
        "approval_status": clip.approval_status or "pending_review",
        "selected_variant": selected_variant,
        "output_url": selected_video_url or clip.output_url,
        "has_video": clip.status == "completed" or bool(versions),
        "status": clip.status
    }


@app.post("/api/local-worker/jobs/{job_id}/upload-video/{clip_index}")
async def local_worker_upload_video(
    job_id: str,
    clip_index: int,
    file: UploadFile = File(...),
    authorized: bool = Depends(verify_local_worker_key)
):
    """
    Upload a generated video from local worker.
    Proxies to R2 to avoid SSL issues on Windows.
    
    Filename format: clip_{clip_index}_{attempt}.{variant}.mp4
    Example: clip_0_1.1.mp4 (clip 0, attempt 1, variant 1)
    
    v507: NO `db: DBSession = Depends(get_db_session)` parameter. The
    R2 upload takes 5-30s. Holding a DB connection that long under
    parallel worker load exhausted the pool. Now we open the connection
    only briefly for the final clip update, AFTER the R2 upload.
    """
    from backends.storage import is_storage_configured, get_storage
    from models import get_db
    import re
    
    if not is_storage_configured():
        raise HTTPException(status_code=503, detail="Storage not configured")
    
    try:
        storage = get_storage()
        
        # Parse filename to extract attempt and variant
        # Expected format: clip_{index}_{attempt}.{variant}.mp4
        filename = file.filename or f"clip_{clip_index}_1.1.mp4"
        match = re.match(r'clip_(\d+)_(\d+)\.(\d+)\.mp4', filename)
        
        if match:
            attempt = int(match.group(2))
            variant = int(match.group(3))
        else:
            # Fallback for old format: clip_{index}.mp4
            attempt = 1
            variant = 1
        
        print(f"[LocalWorker] Uploading clip {clip_index}, attempt {attempt}, variant {variant}", flush=True)
        
        # Save uploaded file temporarily
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        # Upload to R2 with unique key including attempt.variant.
        # NO DB connection held during this slow operation.
        r2_key = f"jobs/{job_id}/outputs/clip_{clip_index}_{attempt}.{variant}.mp4"
        await asyncio.to_thread(storage.upload_file, tmp_path, r2_key, 'video/mp4')
        
        # Generate URL
        output_url = await asyncio.to_thread(storage.get_presigned_url, r2_key, 86400 * 7)
        
        # Clean up temp file
        os.remove(tmp_path)
        
        # === Open brief DB session for the clip update ===
        with get_db() as db:
            # Update clip in database - use FOR UPDATE to prevent race condition
            # between variant 1.1 and 1.2 uploads happening simultaneously
            clip = db.query(Clip).filter(
                Clip.job_id == job_id,
                Clip.clip_index == clip_index
            ).with_for_update().first()
            
            if clip:
                old_status = clip.status
                
                # DEBUG: Log current state before modification
                print(f"[DEBUG-UPLOAD] Clip {clip_index} variant {attempt}.{variant}: old_status={old_status}", flush=True)
                
                # Load existing versions
                versions = json.loads(clip.versions_json) if clip.versions_json else []
                
                # Create version key for this attempt.variant
                version_key = f"{attempt}.{variant}"
                
                # Check if this version already exists (by attempt.variant combo)
                existing_idx = None
                for idx, v in enumerate(versions):
                    v_attempt = v.get("attempt", 1)
                    v_variant = v.get("variant", 1)
                    if v_attempt == attempt and v_variant == variant:
                        existing_idx = idx
                        break
                
                # Create version entry
                version_entry = {
                    "attempt": attempt,
                    "variant": variant,
                    "version_key": version_key,
                    "filename": f"clip_{clip_index}_{attempt}.{variant}.mp4",
                    "url": output_url,
                    "generated_at": datetime.utcnow().isoformat(),
                }
                
                if existing_idx is not None:
                    versions[existing_idx] = version_entry
                else:
                    versions.append(version_entry)
                
                versions.sort(key=lambda x: (x.get("attempt", 1), x.get("variant", 1)))
                
                clip.versions_json = json.dumps(versions)
                
                # Update main output for the primary variant (X.1)
                # If variant 1 failed to upload, fall back to whatever variant DID upload
                if variant == 1:
                    clip.output_url = output_url
                    clip.output_filename = f"clip_{clip_index}_{attempt}.{variant}.mp4"
                    clip.generation_attempt = attempt
                    clip.selected_variant = len([v for v in versions if v.get("attempt") == attempt and v.get("variant") <= variant])
                elif not clip.output_filename:
                    # Variant 1 never made it — use this variant as primary
                    clip.output_url = output_url
                    clip.output_filename = f"clip_{clip_index}_{attempt}.{variant}.mp4"
                    clip.generation_attempt = attempt
                    clip.selected_variant = 1
                    print(f"[DEBUG-UPLOAD] Clip {clip_index}: variant 1 missing, using variant {variant} as primary", flush=True)
                
                # NOTE: Do NOT set clip.status = "completed" or mark job as completed here!
                # The worker will call update_clip_status(clip_id, 'completed') AFTER all variants
                # are uploaded, which properly handles status and job completion.
                
                add_job_log(
                    db, job_id,
                    f"Clip {clip_index + 1} variant {attempt}.{variant} uploaded via Flow backend",
                    "INFO", "flow"
                )
                
                db.commit()
                print(f"[LocalWorker] Uploaded video for clip {clip_index} ({attempt}.{variant}): {r2_key}", flush=True)
        
        return {
            "success": True,
            "key": r2_key,
            "url": output_url,
            "attempt": attempt,
            "variant": variant
        }
        
    except Exception as e:
        print(f"[LocalWorker] Video upload error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


# ============ User Worker API (Self-Hosted Workers) ============
# These endpoints allow users to run their own Flow worker on their machine.
# Each user gets a personal token that scopes all operations to their own jobs.

def verify_user_worker_token(
    authorization: str = Header(None),
    db: DBSession = Depends(get_db_session)
) -> str:
    """Verify user worker token and return user_id.

    v518: throttled last_seen updates. Workers poll this multiple times
    per second across ~11 endpoints, and each poll previously did a
    SELECT + UPDATE + COMMIT to refresh ``token.last_seen``. With many
    workers active that's a write storm against the connection pool —
    every commit holds a session for ~10-50ms and the pool maxes at 90
    connections. Token-verification writes were ~95% of pool traffic
    before this fix.

    The throttle: only write last_seen when it's older than 60 seconds.
    Diagnostic value of "seen 50ms ago" vs "seen 30s ago" is identical
    for human debugging, and the UI's "worker last seen" indicator
    refreshes every minute or two anyway. Skip-write means we just read
    the row and return user_id without touching the session's tx state,
    so the connection releases cleanly without a commit cycle.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing authorization header")
    
    token_value = authorization[7:]
    
    token = db.query(UserWorkerToken).filter(
        UserWorkerToken.id == token_value,
        UserWorkerToken.is_active == True
    ).first()
    
    if not token:
        raise HTTPException(status_code=401, detail="Invalid or revoked worker token")
    
    # v518: only write last_seen if the existing value is stale (>60s).
    # Polling endpoints hit this dependency on every call — without the
    # throttle, the cumulative write load was crashing the pool.
    now = datetime.utcnow()
    if (token.last_seen is None
            or (now - token.last_seen).total_seconds() > 60):
        token.last_seen = now
        db.commit()
    
    return token.user_id


# --- Token Management (called from web UI) ---

@app.post("/api/user-worker/tokens/generate")
async def generate_user_worker_token(
    user: User = Depends(get_current_user),
    db: DBSession = Depends(get_db_session)
):
    """Generate a new worker token for the authenticated user."""
    active_count = db.query(UserWorkerToken).filter(
        UserWorkerToken.user_id == user.id,
        UserWorkerToken.is_active == True
    ).count()
    
    if active_count >= 5:
        raise HTTPException(400, "Maximum 5 active worker tokens. Revoke one first.")
    
    token = UserWorkerToken(
        id=secrets.token_urlsafe(48),
        user_id=user.id,
        name=f"Worker-{datetime.utcnow().strftime('%Y%m%d-%H%M')}",
    )
    db.add(token)
    db.commit()
    
    return {
        "token": token.id,
        "name": token.name,
        "created_at": token.created_at.isoformat(),
    }


@app.get("/api/user-worker/tokens")
async def list_user_worker_tokens(
    user: User = Depends(get_current_user),
    db: DBSession = Depends(get_db_session)
):
    """List all worker tokens for the authenticated user."""
    tokens = db.query(UserWorkerToken).filter(
        UserWorkerToken.user_id == user.id,
        UserWorkerToken.is_active == True
    ).order_by(UserWorkerToken.created_at.desc()).all()
    
    return {"tokens": [t.to_dict() for t in tokens]}


@app.delete("/api/user-worker/tokens/{token_prefix}")
async def revoke_user_worker_token(
    token_prefix: str,
    user: User = Depends(get_current_user),
    db: DBSession = Depends(get_db_session)
):
    """Revoke a worker token by its display prefix."""
    token = db.query(UserWorkerToken).filter(
        UserWorkerToken.user_id == user.id,
        UserWorkerToken.id.like(f"{token_prefix}%")
    ).first()
    
    if not token:
        raise HTTPException(404, "Token not found")
    
    token.is_active = False
    db.commit()
    return {"success": True, "message": "Token revoked"}


# --- Worker Endpoints ---

@app.get("/api/user-worker/health")
async def user_worker_health():
    """Health check for user worker."""
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/api/user-worker/jobs/pending")
async def user_worker_get_pending_job(
    request: Request,
    worker_id: Optional[str] = Query(None),
    exclude: Optional[str] = Query(None),
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token)
):
    """Get next pending Flow job for THIS user only."""
    from sqlalchemy import or_
    
    exclude_ids = [eid.strip() for eid in exclude.split(",") if eid.strip()] if exclude else []
    
    # Release stale claims
    claim_timeout = datetime.utcnow() - timedelta(minutes=10)
    stale_jobs = db.query(Job).filter(
        Job.user_id == user_id,
        Job.backend == 'flow',
        Job.status.in_(['pending', 'queued_for_flow']),
        Job.claimed_by_worker.isnot(None),
        Job.claimed_at < claim_timeout
    ).all()
    
    for stale_job in stale_jobs:
        stale_job.claimed_by_worker = None
        stale_job.claimed_at = None
    if stale_jobs:
        db.commit()
    
    # Query for available jobs - SCOPED TO USER
    if worker_id:
        query = db.query(Job).filter(
            Job.user_id == user_id,
            Job.backend == 'flow',
            Job.status.in_(['pending', 'queued_for_flow']),
            or_(
                Job.claimed_by_worker.is_(None),
                Job.claimed_by_worker == worker_id
            )
        )
        if exclude_ids:
            query = query.filter(Job.id.notin_(exclude_ids))
        
        job = query.order_by(Job.created_at.asc()).first()
        
        if job:
            job.claimed_by_worker = worker_id
            job.claimed_at = datetime.utcnow()
            if job.status == 'pending':
                job.status = 'queued_for_flow'
            db.commit()
    else:
        job = db.query(Job).filter(
            Job.user_id == user_id,
            Job.backend == 'flow',
            Job.status.in_(['pending', 'queued_for_flow']),
            Job.claimed_by_worker.is_(None)
        ).order_by(Job.created_at.asc()).first()
    
    # v455: piggyback abort signals (same as local-worker endpoint)
    aborted_jobs = []
    if worker_id:
        try:
            rows = db.query(Job.id).filter(
                Job.user_id == user_id,
                Job.claimed_by_worker == worker_id,
                Job.abort_requested == True,  # noqa: E712
            ).all()
            aborted_jobs = [r.id for r in rows]
        except Exception as e:
            print(f"[UserWorker] abort_requested lookup failed (migration pending?): {e}", flush=True)
            aborted_jobs = []

    if not job:
        return {"job": None, "aborted_jobs": aborted_jobs}
    
    # Build response (same format as local-worker)
    base_url = str(request.base_url).rstrip('/')
    clips = db.query(Clip).filter(Clip.job_id == job.id).order_by(Clip.clip_index).all()
    
    job_config = {}
    if job.config_json:
        try:
            job_config = json.loads(job.config_json) if isinstance(job.config_json, str) else job.config_json
        except:
            pass
    
    use_interpolation = job_config.get("use_interpolation", True)
    unique_frames = set(c.start_frame for c in clips if c.start_frame)
    single_image_mode = len(unique_frames) <= 1
    
    clips_data = []
    for clip in clips:
        start_frame_key = clip.start_frame
        end_frame_key = clip.end_frame
        
        # Backfill: if clip has no frame keys, compute from job data
        if not start_frame_key and job.frames_storage_keys:
            try:
                _fk = json.loads(job.frames_storage_keys)
                _dd = json.loads(job.dialogue_json) if job.dialogue_json else {}
                _dl = _dd.get("lines", [])
                _uf = sorted(_fk.keys())
                _ni = len(_uf)
                if _ni > 0 and clip.clip_index < len(_dl):
                    _ld = _dl[clip.clip_index]
                    _si = _ld.get("start_image_idx", 0) if isinstance(_ld, dict) else 0
                    _sf = _uf[_si % _ni]
                    start_frame_key = f"jobs/{job.id}/frames/{_sf}"
                    if (clip.clip_mode or "blend") == "blend":
                        end_frame_key = start_frame_key
                    clip.start_frame = start_frame_key
                    if end_frame_key:
                        clip.end_frame = end_frame_key
                    db.commit()
            except Exception:
                pass
        
        start_filename = start_frame_key.split('/')[-1] if start_frame_key else None
        end_filename = end_frame_key.split('/')[-1] if end_frame_key else None
        
        clips_data.append({
            "id": clip.id,
            "clip_index": clip.clip_index,
            "dialogue_text": clip.dialogue_text,
            "prompt": clip.prompt_text,
            "start_frame_key": start_frame_key,
            "end_frame_key": end_frame_key,
            "status": clip.status,
            "start_frame_url": f"{base_url}/api/user-worker/frames/{job.id}/{start_filename}" if start_filename else None,
            "end_frame_url": f"{base_url}/api/user-worker/frames/{job.id}/{end_filename}" if end_filename else None,
            "clip_mode": clip.clip_mode or "blend",
            "scene_index": clip.scene_index or 0,
        })
    
    return {
        "job": {
            "id": job.id,
            "aspect_ratio": job_config.get("aspect_ratio", "9:16"),
            "duration": job_config.get("duration", "8"),
            "language": job_config.get("language", "English"),
            "voice_profile": job_config.get("voice_profile", "") or job_config.get("user_context", ""),
            "resolution": job_config.get("resolution", "720p"),
            "use_interpolation": use_interpolation,
            "single_image_mode": single_image_mode,
            "flow_project_url": job.flow_project_url,
            "flow_variants_count": job_config.get("flow_variants_count", 2),
            "short_dialogue_mode": job_config.get("short_dialogue_mode", "optimized"),
            "prefix_short_enabled": job_config.get("prefix_short_enabled", False),
            "prefix_short_word": job_config.get("prefix_short_word", "only"),
            "prefix_short_threshold": job_config.get("prefix_short_threshold", 15),
            "clips": clips_data,
            "claimed_by": job.claimed_by_worker,
        },
        "aborted_jobs": aborted_jobs,
    }


@app.get("/api/user-worker/clips/redo-pending")
async def user_worker_get_redo_clips(
    request: Request,
    worker_id: Optional[str] = Query(None),
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token)
):
    """Get clips needing regeneration for THIS user's Flow jobs."""
    from sqlalchemy import or_, and_
    
    claim_timeout = datetime.utcnow() - timedelta(minutes=10)
    stale_clips = db.query(Clip).join(Job).filter(
        Job.user_id == user_id,
        Job.backend == 'flow',
        Clip.status == ClipStatus.FLOW_REDO_QUEUED.value,
        Clip.claimed_by_worker.isnot(None),
        Clip.claimed_at < claim_timeout
    ).all()
    
    # v468: zombie-loop prevention (mirror of local-worker endpoint).
    # Clips stuck in flow_redo_queued for >30min have been re-claimed
    # 3+ times without ever completing. Break the loop.
    ZOMBIE_THRESHOLD_MINUTES = 30
    _zombie_count = 0
    for stale_clip in stale_clips:
        _redo_started = stale_clip.claimed_at or stale_clip.updated_at
        if _redo_started and (datetime.utcnow() - _redo_started) > timedelta(minutes=ZOMBIE_THRESHOLD_MINUTES):
            print(f"[UserWorker] ⛔ Clip {stale_clip.id} stuck in redo for >{ZOMBIE_THRESHOLD_MINUTES}min — marking failed", flush=True)
            stale_clip.status = ClipStatus.FAILED.value
            stale_clip.claimed_by_worker = None
            stale_clip.claimed_at = None
            stale_clip.approval_status = "pending_review"
            stale_clip.error_code = "REDO_ZOMBIE"
            stale_clip.error_message = f"Redo stuck: worker claimed this clip repeatedly over {ZOMBIE_THRESHOLD_MINUTES}+ minutes without completing. Click Retry to try again, or remove the clip."
            try:
                add_job_log(db, stale_clip.job_id,
                            f"Clip {stale_clip.clip_index + 1} marked failed — redo zombie loop (>{ZOMBIE_THRESHOLD_MINUTES}min)",
                            "ERROR", "redo_zombie")
            except Exception:
                pass
            _zombie_count += 1
            continue
        stale_clip.claimed_by_worker = None
        stale_clip.claimed_at = None
    if stale_clips:
        db.commit()
        if _zombie_count:
            print(f"[UserWorker] Marked {_zombie_count} zombie redo(s) as failed; released {len(stale_clips) - _zombie_count} transient stale claim(s)", flush=True)
        else:
            print(f"[UserWorker] Released {len(stale_clips)} stale redo claim(s)", flush=True)
    
    if worker_id:
        # v487: reverted Clip.updated_at reference — column doesn't
        # exist, see local-worker endpoint comment.
        redo_cutoff = datetime.utcnow() - timedelta(hours=24)
        redo_clips = db.query(Clip).join(Job).filter(
            Job.user_id == user_id,
            Job.backend == 'flow',
            Job.updated_at >= redo_cutoff,
            or_(
                and_(
                    Clip.status == ClipStatus.FLOW_REDO_QUEUED.value,
                    or_(Clip.claimed_by_worker.is_(None), Clip.claimed_by_worker == worker_id)
                ),
                and_(
                    Clip.status == 'failed',
                    Clip.generation_attempt > 1,
                    Clip.error_message.ilike('%file not found%')
                )
            )
        ).order_by(Clip.id.asc()).all()
    else:
        redo_cutoff = datetime.utcnow() - timedelta(hours=24)
        redo_clips = db.query(Clip).join(Job).filter(
            Job.user_id == user_id,
            Job.backend == 'flow',
            Job.updated_at >= redo_cutoff,
            or_(
                and_(Clip.status == ClipStatus.FLOW_REDO_QUEUED.value, Clip.claimed_by_worker.is_(None)),
                and_(Clip.status == 'failed', Clip.generation_attempt > 1, Clip.error_message.ilike('%file not found%'))
            )
        ).order_by(Clip.id.asc()).all()
    
    if not redo_clips:
        return {"clips": []}
    
    base_url = str(request.base_url).rstrip('/')
    clips_data = []
    for clip in redo_clips:
        job = clip.job
        
        if clip.status == 'failed':
            clip.status = ClipStatus.FLOW_REDO_QUEUED.value
            clip.error_message = None
            db.commit()
        
        if worker_id and clip.claimed_by_worker != worker_id:
            clip.claimed_by_worker = worker_id
            clip.claimed_at = datetime.utcnow()
            # NOTE: Do NOT change status to 'generating' here. The worker changes
            # it when it actually starts processing (in process_redo_clip).
            # If we change it here and the worker drops the redo (golden restore,
            # queue lost, thread crashed), the clip is permanently lost — it's
            # no longer flow_redo_queued so it won't be re-polled, and the stale
            # claim release only checks flow_redo_queued status.
            db.commit()
            add_job_log(db, clip.job_id, f"Flow redo for clip {clip.clip_index + 1} claimed by user worker", "INFO", "redo")
        
        start_frame_key = clip.start_frame
        end_frame_key = clip.end_frame
        
        # Fallback: if clip has no frame keys (API job redirected to Flow),
        # compute them from job's frames_storage_keys + dialogue data
        if not start_frame_key and job.frames_storage_keys:
            try:
                frames_keys = json.loads(job.frames_storage_keys)
                dialogue_data = json.loads(job.dialogue_json) if job.dialogue_json else {}
                dialogue_lines = dialogue_data.get("lines", [])
                uploaded_frames = sorted(frames_keys.keys())
                num_images = len(uploaded_frames)
                
                if num_images > 0 and clip.clip_index < len(dialogue_lines):
                    line_data = dialogue_lines[clip.clip_index]
                    if isinstance(line_data, dict):
                        start_img_idx = line_data.get("start_image_idx", 0)
                    else:
                        start_img_idx = 0
                    
                    start_fname = uploaded_frames[start_img_idx % num_images]
                    start_frame_key = f"jobs/{job.id}/frames/{start_fname}"
                    
                    # Also set end frame for blend mode
                    clip_mode = clip.clip_mode or "blend"
                    if clip_mode == "blend":
                        end_frame_key = start_frame_key
                    
                    # Persist to DB so future redos don't need fallback
                    clip.start_frame = start_frame_key
                    if end_frame_key:
                        clip.end_frame = end_frame_key
                    db.commit()
                    print(f"[Redo] Backfilled frame keys for clip {clip.clip_index}: start={start_fname}", flush=True)
            except Exception as _fb_err:
                print(f"[Redo] Frame key backfill failed: {_fb_err}", flush=True)
        
        start_filename = start_frame_key.split('/')[-1] if start_frame_key else None
        end_filename = end_frame_key.split('/')[-1] if end_frame_key else None
        
        job_config = {}
        if job.config_json:
            try:
                job_config = json.loads(job.config_json) if isinstance(job.config_json, str) else job.config_json
            except:
                pass
        
        clips_data.append({
            "id": clip.id, "job_id": job.id, "clip_index": clip.clip_index,
            "dialogue_text": clip.dialogue_text, "prompt": clip.prompt_text,
            "language": job_config.get("language", "English"),
            "duration": job_config.get("duration", "8"),
            "voice_profile": job_config.get("voice_profile", "") or job_config.get("user_context", ""),
            "start_frame_key": start_frame_key,
            "end_frame_key": end_frame_key,
            "start_frame_url": f"{base_url}/api/user-worker/frames/{job.id}/{start_filename}" if start_filename else None,
            "end_frame_url": f"{base_url}/api/user-worker/frames/{job.id}/{end_filename}" if end_filename else None,
            "flow_project_url": job.flow_project_url,
            "generation_attempt": clip.generation_attempt,
            "redo_reason": clip.redo_reason,
            "claimed_by": clip.claimed_by_worker,
            "clip_mode": clip.clip_mode or "blend",
            "scene_index": clip.scene_index or 0,
            "short_dialogue_mode": job_config.get("short_dialogue_mode", "optimized"),
            "prefix_short_enabled": job_config.get("prefix_short_enabled", False),
            "prefix_short_word": job_config.get("prefix_short_word", "only"),
            "prefix_short_threshold": job_config.get("prefix_short_threshold", 15),
            "flow_variants_count": job_config.get("flow_variants_count", 2),
        })
    
    return {"clips": clips_data}


@app.get("/api/user-worker/jobs/{job_id}")
async def user_worker_get_job(
    job_id: str,
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token)
):
    """Get job details including clip statuses — for worker dedup and status checks."""
    job = db.query(Job).filter(Job.id == job_id, Job.user_id == user_id).first()
    if not job:
        raise HTTPException(404, "Job not found")
    clips = db.query(Clip).filter(Clip.job_id == job_id).order_by(Clip.clip_index.asc()).all()
    return {
        "id": job.id,
        "status": job.status,
        "progress_percent": job.progress_percent,
        "total_clips": job.total_clips,
        "completed_clips": job.completed_clips,
        "failed_clips": job.failed_clips,
        "clips": [{"id": c.id, "clip_index": c.clip_index, "status": c.status,
                   "output_filename": c.output_filename, "approval_status": c.approval_status}
                  for c in clips]
    }


@app.post("/api/user-worker/jobs/{job_id}/status")
async def user_worker_update_job_status(
    job_id: str,
    update: LocalWorkerJobUpdate,
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token)
):
    """Update job status - verified ownership."""
    job = db.query(Job).filter(Job.id == job_id, Job.user_id == user_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or not yours")
    
    if update.status:
        job.status = update.status
        if update.status in ['completed', 'failed', 'cancelled']:
            job.claimed_by_worker = None
            job.claimed_at = None
    if update.error_message:
        job.error_message = update.error_message
    if update.flow_project_url:
        job.flow_project_url = update.flow_project_url
    job.updated_at = datetime.utcnow()
    
    db.commit()
    return {"success": True, "job_id": job_id, "status": job.status}


@app.post("/api/user-worker/clips/{clip_id}/status")
async def user_worker_update_clip_status(
    clip_id: str,
    update: LocalWorkerClipUpdate,
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token)
):
    """Update clip status - verified ownership."""
    clip = db.query(Clip).join(Job).filter(Clip.id == clip_id, Job.user_id == user_id).with_for_update().first()
    if not clip:
        raise HTTPException(status_code=404, detail="Clip not found or not yours")
    
    job = db.query(Job).filter(Job.id == clip.job_id).first()
    if job and job.backend == 'flow' and job.images_dir:
        job.images_dir = ""
    
    old_status = clip.status
    
    if update.status:
        clip.status = update.status
        if update.status in ['completed', 'failed']:
            clip.claimed_by_worker = None
            clip.claimed_at = None
            if update.status == 'failed' and old_status == ClipStatus.GENERATING.value:
                error_msg = update.error_message or "Unknown error"
        # v464: symmetric to local-worker endpoint — reset approval
        # when a clip goes back to non-completed so UI label + summary
        # count stay consistent. See rationale in local_worker_update_clip_status.
        if update.status != 'completed' and clip.approval_status == 'approved':
            clip.approval_status = 'pending_review'
        if update.status == 'flow_redo_queued':
            clip.claimed_by_worker = None
            clip.claimed_at = None
            if job and job.status == 'completed':
                job.status = 'processing'
                job.completed_at = None
                _done = db.query(Clip).filter(
                    Clip.job_id == clip.job_id,
                    Clip.status == ClipStatus.COMPLETED.value
                ).count()
                job.completed_clips = _done
                if job.total_clips > 0:
                    job.progress_percent = int((_done / job.total_clips) * 100)
                add_job_log(db, clip.job_id, f"Flow redo clip {clip.clip_index + 1} re-queued for redo", "INFO", "redo")
        if update.status != 'failed':
            clip.error_message = None
            clip.error_code = None
    
    if update.output_url:
        clip.output_url = update.output_url
    if update.output_key:
        clip.output_key = update.output_key
    if update.error_message:
        clip.error_message = update.error_message
    
    if update.status == 'completed' and old_status in ['generating', 'redo_queued', 'flow_redo_queued']:
        clip.approval_status = 'pending_review'
        clip.completed_at = datetime.utcnow()
        
        if update.output_url:
            import re as re_mod
            match = re_mod.search(r'/outputs/([^/]+\.mp4)', update.output_url)
            if match:
                clip.output_filename = match.group(1)
        
        versions = json.loads(clip.versions_json) if clip.versions_json else []
        if versions:
            clip.selected_variant = 1
        
        add_job_log(db, clip.job_id, f"Clip {clip.clip_index + 1} completed via user worker", "INFO", "flow")
        
        if job:
            completed = db.query(Clip).filter(Clip.job_id == clip.job_id, Clip.status == ClipStatus.COMPLETED.value).count()
            job.completed_clips = completed
            if job.total_clips > 0:
                job.progress_percent = int((completed / job.total_clips) * 100)
            if completed >= job.total_clips:
                job.status = "completed"
                job.completed_at = datetime.utcnow()
    
    clip.updated_at = datetime.utcnow()
    db.commit()
    return {"success": True, "clip_id": clip_id, "status": clip.status}


@app.get("/api/user-worker/frames/{job_id}/{filename}")
async def user_worker_download_frame(
    job_id: str,
    filename: str,
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token)
):
    """Download a frame - verified ownership.
    
    v507: release DB connection before the slow R2 download. Previously
    held the connection for the entire 2-10s download, contributing to
    pool exhaustion under parallel worker activity.
    """
    job = db.query(Job).filter(Job.id == job_id, Job.user_id == user_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or not yours")
    # v507: release the pooled connection NOW — no more DB work in this function
    db.close()
    
    from backends.storage import is_storage_configured, get_storage
    
    r2_key = f"jobs/{job_id}/frames/{filename}"
    
    local_path = app_config.uploads_dir / job_id / filename
    if local_path.exists():
        with open(local_path, 'rb') as f:
            content = f.read()
        media_type = 'image/png' if filename.endswith('.png') else 'image/jpeg'
        return Response(content=content, media_type=media_type)
    
    if not is_storage_configured():
        raise HTTPException(status_code=503, detail="Storage not configured")
    
    try:
        storage = get_storage()
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmp:
            tmp_path = tmp.name
        # Wrap in asyncio.to_thread so event loop stays responsive
        await asyncio.to_thread(storage.download_file, r2_key, tmp_path)
        with open(tmp_path, 'rb') as f:
            content = f.read()
        os.remove(tmp_path)
        media_type = 'image/png' if filename.endswith('.png') else 'image/jpeg'
        return Response(content=content, media_type=media_type)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Frame not found: {filename}")


@app.post("/api/user-worker/enhance-frame")
async def user_worker_enhance_frame(
    request_body: EnhanceFrameRequest,
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token)
):
    """Enhance frame - verified ownership.
    
    v507: release DB connection before the slow enhance call (10-30s).
    """
    job = db.query(Job).filter(Job.id == request_body.job_id, Job.user_id == user_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or not yours")
    # v507: no more DB work in this function — release connection before slow I/O
    db.close()
    
    import base64
    import tempfile
    from backends.storage import is_storage_configured, get_storage
    
    try:
        frame_bytes = base64.b64decode(request_body.frame_base64)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp:
            tmp.write(frame_bytes)
            frame_path = Path(tmp.name)
        
        original_scene_path = None
        if request_body.original_frame_key and is_storage_configured():
            try:
                storage = get_storage()
                with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as orig_tmp:
                    original_scene_path = Path(orig_tmp.name)
                storage.download_file(request_body.original_frame_key, str(original_scene_path))
            except:
                original_scene_path = None
        
        enhanced_path = _enhance_frame_with_nano_banana(frame_path, original_scene_path)
        
        with open(enhanced_path or frame_path, 'rb') as f:
            result_bytes = f.read()
        
        try:
            frame_path.unlink()
            if original_scene_path and original_scene_path.exists():
                original_scene_path.unlink()
            if enhanced_path and enhanced_path != frame_path and enhanced_path.exists():
                enhanced_path.unlink()
        except:
            pass
        
        result_base64 = base64.b64encode(result_bytes).decode('utf-8')
        return {"success": True, "enhanced": enhanced_path is not None, "frame_base64": result_base64}
    except Exception as e:
        return {"success": False, "enhanced": False, "frame_base64": request_body.frame_base64, "error": str(e)}


@app.get("/api/user-worker/clips/{clip_id}/approval-status")
async def user_worker_get_clip_approval_status(
    clip_id: int,
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token)
):
    """Get clip approval status - verified ownership."""
    clip = db.query(Clip).join(Job).filter(Clip.id == clip_id, Job.user_id == user_id).first()
    if not clip:
        raise HTTPException(status_code=404, detail="Clip not found or not yours")
    
    versions = []
    if clip.versions_json:
        try:
            versions = json.loads(clip.versions_json) if isinstance(clip.versions_json, str) else clip.versions_json
        except:
            pass
    
    selected_variant = clip.selected_variant or 1
    selected_video_url = None
    for v in versions:
        if v.get('attempt') == 1 and v.get('variant') == selected_variant:
            selected_video_url = v.get('url')
            break
        if v.get('attempt') == selected_variant and not v.get('variant'):
            selected_video_url = v.get('url')
            break
    
    return {
        "clip_id": clip.id, "clip_index": clip.clip_index,
        "approval_status": clip.approval_status or "pending_review",
        "selected_variant": selected_variant,
        "output_url": selected_video_url or clip.output_url,
        "has_video": clip.status == "completed" or bool(versions),
        "status": clip.status,
    }


@app.post("/api/user-worker/jobs/{job_id}/upload-video/{clip_index}")
async def user_worker_upload_video(
    job_id: str,
    clip_index: int,
    file: UploadFile = File(...),
    user_id: str = Depends(verify_user_worker_token)
):
    """Upload a generated video - verified ownership.
    
    v507: NO `db: DBSession = Depends(get_db_session)` parameter. The
    previous version held a DB connection for the entire request, which
    spans a 5-30s R2 upload. Under parallel worker activity that
    exhausted the 90-connection pool within minutes ("QueuePool limit
    of size 30 overflow 60 reached, connection timed out").
    
    Now we open the DB connection only for the brief query/update
    moments, NOT during the R2 upload. Hold time goes from ~30s to
    ~50ms per request.
    """
    from backends.storage import is_storage_configured, get_storage
    import re as re_mod
    from models import get_db
    
    if not is_storage_configured():
        raise HTTPException(status_code=503, detail="Storage not configured")
    
    # === Brief DB session 1: verify ownership ===
    with get_db() as db:
        job = db.query(Job).filter(Job.id == job_id, Job.user_id == user_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found or not yours")
    
    # === No DB held: read file + upload to R2 ===
    try:
        storage = get_storage()
        
        filename = file.filename or f"clip_{clip_index}_1.1.mp4"
        match = re_mod.match(r'clip_(\d+)_(\d+)\.(\d+)\.mp4', filename)
        attempt = int(match.group(2)) if match else 1
        variant = int(match.group(3)) if match else 1
        
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        r2_key = f"jobs/{job_id}/outputs/clip_{clip_index}_{attempt}.{variant}.mp4"
        # asyncio.to_thread frees the event loop while R2 uploads.
        await asyncio.to_thread(storage.upload_file, tmp_path, r2_key, 'video/mp4')
        output_url = await asyncio.to_thread(storage.get_presigned_url, r2_key, 86400 * 7)
        os.remove(tmp_path)
        
        # === Brief DB session 2: write clip metadata ===
        with get_db() as db:
            clip = db.query(Clip).filter(
                Clip.job_id == job_id, Clip.clip_index == clip_index
            ).with_for_update().first()
            if clip:
                versions = json.loads(clip.versions_json) if clip.versions_json else []
                
                existing_idx = None
                for idx, v in enumerate(versions):
                    if v.get("attempt") == attempt and v.get("variant") == variant:
                        existing_idx = idx
                        break
                
                version_entry = {
                    "attempt": attempt, "variant": variant,
                    "version_key": f"{attempt}.{variant}",
                    "filename": f"clip_{clip_index}_{attempt}.{variant}.mp4",
                    "url": output_url,
                    "generated_at": datetime.utcnow().isoformat(),
                }
                
                if existing_idx is not None:
                    versions[existing_idx] = version_entry
                else:
                    versions.append(version_entry)
            
            versions.sort(key=lambda x: (x.get("attempt", 1), x.get("variant", 1)))
            clip.versions_json = json.dumps(versions)
            
            if variant == 1:
                clip.output_url = output_url
                clip.output_filename = f"clip_{clip_index}_{attempt}.{variant}.mp4"
                clip.generation_attempt = attempt
                clip.selected_variant = len([v for v in versions if v.get("attempt") == attempt and v.get("variant") <= variant])
            elif not clip.output_filename:
                clip.output_url = output_url
                clip.output_filename = f"clip_{clip_index}_{attempt}.{variant}.mp4"
                clip.generation_attempt = attempt
                clip.selected_variant = 1
            
            add_job_log(db, job_id, f"Clip {clip_index + 1} variant {attempt}.{variant} uploaded via user worker", "INFO", "flow")
            db.commit()
        
        return {"success": True, "key": r2_key, "url": output_url, "attempt": attempt, "variant": variant}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


# --- Worker Setup File Serving ---

@app.get("/api/user-worker/download/setup.sh")
async def serve_setup_bash():
    """Serve bash bootstrap script."""
    setup_path = Path(__file__).parent / "static" / "setup.sh"
    if not setup_path.exists():
        raise HTTPException(404, "Setup script not found")
    return Response(content=setup_path.read_text(), media_type="text/plain")


@app.get("/api/user-worker/download/setup.ps1")
async def serve_setup_powershell():
    """Serve PowerShell bootstrap script."""
    setup_path = Path(__file__).parent / "static" / "setup.ps1"
    if not setup_path.exists():
        raise HTTPException(404, "Setup script not found")
    return Response(content=setup_path.read_text(), media_type="text/plain")


@app.get("/api/user-worker/download/setup_worker.py")
async def serve_setup_worker():
    """Serve the Python setup script."""
    setup_path = Path(__file__).parent / "static" / "setup_worker.py"
    if not setup_path.exists():
        raise HTTPException(404, "Setup script not found")
    return Response(content=setup_path.read_text(), media_type="text/x-python")


@app.get("/api/user-worker/download/flow_worker.py")
async def serve_flow_worker():
    """Serve the latest flow worker script."""
    worker_path = Path(__file__).parent / "static" / "flow_worker.py"
    if not worker_path.exists():
        raise HTTPException(404, "Worker script not found")
    return Response(content=worker_path.read_text(), media_type="text/x-python")


@app.get("/api/user-worker/version")
async def worker_version():
    """Return current worker version (content hash) for auto-update checks."""
    import hashlib
    worker_path = Path(__file__).parent / "static" / "flow_worker.py"
    if worker_path.exists():
        content_hash = hashlib.md5(worker_path.read_bytes()).hexdigest()[:12]
        return {"version": content_hash}
    return {"version": "unknown"}


@app.get("/api/user-worker/debug")
async def user_worker_debug(
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token)
):
    """Debug endpoint to check user_id and job matching."""
    # Get all flow jobs for this user
    user_flow_jobs = db.query(Job).filter(
        Job.user_id == user_id,
        Job.backend == 'flow'
    ).order_by(Job.created_at.desc()).limit(5).all()
    
    # Get ALL recent flow jobs (any user) for comparison
    all_flow_jobs = db.query(Job).filter(
        Job.backend == 'flow'
    ).order_by(Job.created_at.desc()).limit(5).all()
    
    return {
        "token_user_id": user_id,
        "user_flow_jobs": [{"id": j.id[:8], "status": j.status, "user_id": j.user_id, "created": j.created_at.isoformat()} for j in user_flow_jobs],
        "all_flow_jobs": [{"id": j.id[:8], "status": j.status, "user_id": j.user_id[:8] if j.user_id else None, "created": j.created_at.isoformat()} for j in all_flow_jobs],
    }


@app.get("/api/user-worker/setup-info")
async def user_worker_setup_info(
    user: User = Depends(get_current_user),
    db: DBSession = Depends(get_db_session)
):
    """Get setup info for the worker setup page."""
    tokens = db.query(UserWorkerToken).filter(
        UserWorkerToken.user_id == user.id,
        UserWorkerToken.is_active == True
    ).order_by(UserWorkerToken.created_at.desc()).all()
    
    return {
        "tokens": [t.to_dict() for t in tokens],
        "worker_version": "1.0.0",
    }


# In-memory worker errors (per user) — reset on deploy
_worker_errors = {}  # user_id -> {"error_type": str, "message": str, "account_name": str, "timestamp": datetime}


@app.get("/api/user-worker/status")
async def user_worker_combined_status(
    user: User = Depends(get_current_user),
    db: DBSession = Depends(get_db_session)
):
    """Combined status endpoint for the My Worker dashboard.
    
    Returns online/offline status, current job progress, and token info
    in a single call so the dashboard can poll efficiently.
    """
    # Get active tokens
    tokens = db.query(UserWorkerToken).filter(
        UserWorkerToken.user_id == user.id,
        UserWorkerToken.is_active == True
    ).all()
    
    has_tokens = len(tokens) > 0
    online = False
    last_seen = None
    
    for t in tokens:
        if t.last_seen:
            if last_seen is None or t.last_seen > last_seen:
                last_seen = t.last_seen
            if (datetime.utcnow() - t.last_seen).total_seconds() < 30:
                online = True
    
    # Check for active job — only show if updated recently (not stale from a previous run)
    current_job = None
    if online:
        recent_cutoff = datetime.utcnow() - timedelta(minutes=10)
        job = db.query(Job).filter(
            Job.user_id == user.id,
            Job.backend == 'flow',
            Job.status == 'processing',
            Job.updated_at >= recent_cutoff
        ).order_by(Job.updated_at.desc()).first()
        
        if job:
            clips = db.query(Clip).filter(Clip.job_id == job.id).all()
            completed = sum(1 for c in clips if c.status in ('completed', 'approved'))
            current_job = {
                "id": job.id,
                "clips_total": len(clips),
                "clips_completed": completed,
            }
    
    # Check for recent worker errors
    worker_error = _worker_errors.get(user.id)
    if worker_error:
        # Auto-expire after 10 minutes
        if (datetime.utcnow() - worker_error["timestamp"]).total_seconds() > 600:
            _worker_errors.pop(user.id, None)
            worker_error = None
    
    return {
        "has_tokens": has_tokens,
        "online": online,
        "last_seen": last_seen.isoformat() if last_seen else None,
        "current_job": current_job,
        "worker_error": {
            "error_type": worker_error["error_type"],
            "message": worker_error["message"],
            "account_name": worker_error.get("account_name", ""),
        } if worker_error else None,
    }


@app.post("/api/user-worker/worker-error")
async def report_worker_error(
    request: Request,
    user_id: str = Depends(verify_user_worker_token),
):
    """Worker reports an error (e.g. non-ULTRA account) for the dashboard to display."""
    body = await request.json()
    _worker_errors[user_id] = {
        "error_type": body.get("error_type", "unknown"),
        "message": body.get("message", "Unknown worker error"),
        "account_name": body.get("account_name", ""),
        "timestamp": datetime.utcnow(),
    }
    return {"ok": True}


@app.delete("/api/user-worker/worker-error")
async def clear_worker_error(
    user: User = Depends(get_current_user),
):
    """Clear worker error from dashboard."""
    _worker_errors.pop(user.id, None)
    return {"ok": True}


@app.get("/api/user-worker/download/installer")
async def download_installer(
    request: Request,
    os: str = Query("windows", regex="^(windows|mac|linux)$"),
    accounts: int = Query(1, ge=1, le=4),
    reset: int = Query(0),
    update_only: int = Query(0),
    user: User = Depends(get_current_user),
    db: DBSession = Depends(get_db_session)
):
    """Generate OS-specific installer with user's token baked in.
    
    Settings from the web UI are baked into the installer:
    - accounts: number of Chrome windows
    - reset: 1 = wipe session folders for fresh Google login
    - update_only: 1 = only re-download flow_worker.py, keep everything else
    """
    # Get or create token
    token = db.query(UserWorkerToken).filter(
        UserWorkerToken.user_id == user.id,
        UserWorkerToken.is_active == True
    ).first()
    
    if not token:
        token = UserWorkerToken(
            id=secrets.token_urlsafe(48),
            user_id=user.id,
            name=f"Worker-{datetime.utcnow().strftime('%Y%m%d-%H%M')}",
        )
        db.add(token)
        db.commit()
    
    app_url = str(request.base_url).rstrip('/')
    if 'kavenobuilder.com' not in app_url:
        app_url = "https://kavenobuilder.com"
    
    if os == "windows":
        content = _generate_windows_installer(token.id, app_url, accounts, bool(reset), bool(update_only))
        filename = "KavenoBuilder-Worker-Setup.bat"
        media_type = "application/x-bat"
        return Response(
            content=content,
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Cache-Control": "no-store",
            }
        )
    else:
        # Mac/Linux: wrap .command in a .zip to preserve execute permissions
        # Browsers strip execute bits on download; zips preserve them
        import zipfile, io
        content = _generate_unix_installer(token.id, app_url, accounts, bool(reset), bool(update_only))
        inner_filename = "KavenoBuilder-Worker-Setup.command"
        
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            info = zipfile.ZipInfo(inner_filename)
            # Set Unix execute permission (rwxr-xr-x = 0o755)
            info.external_attr = 0o755 << 16
            info.create_system = 3  # Unix
            zf.writestr(info, content)
        
        return Response(
            content=buf.getvalue(),
            media_type="application/zip",
            headers={
                "Content-Disposition": "attachment; filename=KavenoBuilder-Worker-Setup.zip",
                "Cache-Control": "no-store",
            }
        )


def _generate_windows_installer(token: str, app_url: str, accounts: int = 1, reset: bool = False, update_only: bool = False) -> str:
    """Generate a Windows .bat — simple, one process, everything direct. This is the approach that works."""
    
    env_accounts = "ACCOUNT1_ENABLED=true"
    for n in range(2, accounts + 1):
        env_accounts += f"\nACCOUNT{n}_ENABLED=true"
    for n in range(accounts + 1, 5):
        env_accounts += f"\nACCOUNT{n}_ENABLED=false"
    
    multi = "true" if accounts > 1 else "false"
    
    folder_cmds = 'mkdir "%WORKER_DIR%\\chrome-session" 2>nul\nmkdir "%WORKER_DIR%\\chrome-download" 2>nul'
    for n in range(2, accounts + 1):
        folder_cmds += f'\nmkdir "%WORKER_DIR%\\chrome-session-{n}" 2>nul'
        folder_cmds += f'\nmkdir "%WORKER_DIR%\\chrome-download-{n}" 2>nul'
    
    reset_cmds = ''
    if reset:
        # Wipe ALL session folders (not just up to N) — prevents auto-detection picking up leftovers
        reset_cmds = 'echo   Resetting sessions...\n'
        reset_cmds += 'if exist "%WORKER_DIR%\\chrome-session" rmdir /s /q "%WORKER_DIR%\\chrome-session" 2>nul\n'
        reset_cmds += 'if exist "%WORKER_DIR%\\chrome-golden" rmdir /s /q "%WORKER_DIR%\\chrome-golden" 2>nul\n'
        for n in range(2, 5):  # Always wipe all 4
            reset_cmds += f'if exist "%WORKER_DIR%\\chrome-session-{n}" rmdir /s /q "%WORKER_DIR%\\chrome-session-{n}" 2>nul\n'
            reset_cmds += f'if exist "%WORKER_DIR%\\chrome-golden-{n}" rmdir /s /q "%WORKER_DIR%\\chrome-golden-{n}" 2>nul\n'
        reset_cmds += 'echo   [OK] Sessions reset\necho.'
    
    if update_only:
        return f'''@echo off
setlocal enabledelayedexpansion
title KavenoBuilder Worker
mode con: cols=56 lines=14
color 1F
echo.
echo   KavenoBuilder — Updating Worker
echo.
set "WORKER_DIR=%USERPROFILE%\\veo-worker"
echo   Downloading latest worker...
powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '{app_url}/api/user-worker/download/flow_worker.py' -OutFile '%WORKER_DIR%\\flow_worker.py' -UseBasicParsing" >nul 2>nul
echo   [OK] Updated
echo.
echo   Restart your worker to use the new version.
timeout /t 4 /nobreak >nul
exit
'''
    
    return f'''@echo off
setlocal enabledelayedexpansion
title KavenoBuilder Worker
mode con: cols=56 lines=26
color 1F

echo.
echo   ======================================================
echo    KavenoBuilder Worker Setup
echo   ======================================================
echo.

set "TOKEN={token}"
set "APP_URL={app_url}"
set "WORKER_DIR=%USERPROFILE%\\veo-worker"
set "PY="

echo   [1/5] Finding Python...

where py >nul 2>nul
if not errorlevel 1 (
    for /f "tokens=*" %%p in ('py -c "import sys; print(sys.executable)" 2^>nul') do set "PY=%%p"
    if defined PY goto :found_py
)
where python >nul 2>nul
if not errorlevel 1 (
    set "PY=python"
    goto :found_py
)
for %%v in (313 312 311 310 39) do (
    if exist "%LOCALAPPDATA%\\Programs\\Python\\Python%%v\\python.exe" (
        set "PY=%LOCALAPPDATA%\\Programs\\Python\\Python%%v\\python.exe"
        goto :found_py
    )
)
echo         Not found — installing via winget...
where winget >nul 2>nul
if errorlevel 1 (
    echo.
    echo   ERROR: Python not found. Install from python.org/downloads
    echo   Then double-click this file again.
    echo.
    pause
    exit /b 1
)
winget install Python.Python.3.12 --accept-package-agreements --accept-source-agreements >nul 2>nul
set "PATH=%LOCALAPPDATA%\\Programs\\Python\\Python312;%LOCALAPPDATA%\\Programs\\Python\\Python312\\Scripts;%PATH%"
set "PY=python"

:found_py
echo         OK
{reset_cmds}
echo   [2/5] Installing packages (may take a minute)...
!PY! -m pip install patchright requests --quiet --disable-pip-version-check 2>nul
if errorlevel 1 (
    !PY! -m pip install patchright requests --quiet --user --disable-pip-version-check 2>nul
)
echo         OK

echo   [3/5] Downloading worker...
mkdir "%WORKER_DIR%" 2>nul
{folder_cmds}
powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '%APP_URL%/api/user-worker/download/flow_worker.py' -OutFile '%WORKER_DIR%\\flow_worker.py' -UseBasicParsing" >nul 2>nul
echo         OK

echo   [4/5] Writing config...
(
echo WORKER_MODE=user
echo USER_WORKER_TOKEN=%TOKEN%
echo WEB_APP_URL=%APP_URL%
echo SESSION_FOLDER=%WORKER_DIR%\\chrome-session
echo DOWNLOAD_SESSION_FOLDER=%WORKER_DIR%\\chrome-download
echo BROWSER_MODE=stealth
echo MULTI_ACCOUNT={multi}
echo MULTI_ACCOUNT_MODE={multi}
echo PROXY_TYPE=none
echo {env_accounts}
) > "%WORKER_DIR%\\.env"
echo         OK

echo   [5/5] Registering launcher...
reg add "HKCU\\Software\\Classes\\kavenobuilder" /ve /d "URL:KavenoBuilder Worker" /f >nul 2>nul
reg add "HKCU\\Software\\Classes\\kavenobuilder" /v "URL Protocol" /d "" /f >nul 2>nul
reg add "HKCU\\Software\\Classes\\kavenobuilder\\shell\\open\\command" /ve /d "cmd /c cd /d \\"%WORKER_DIR%\\" ^& for /f \\"usebackq tokens=1,* delims=^\\" %%a in (\\".env\\") do @set \\"%%a=%%b\\" ^& !PY! flow_worker.py" /f >nul 2>nul
echo         OK

echo.
echo   ======================================================
echo    Setup complete! Starting worker...
echo   ======================================================
echo.
echo   Chrome will open — click "Continue without signing in" if prompted,
echo   then log into your Google account on the Flow page.
echo   This window will stay open while the worker runs.
echo   Check status: {app_url}/static/my-worker.html?v=214
echo.

:: Load .env and run worker — all in THIS process
cd /d "%WORKER_DIR%"
for /f "usebackq tokens=1,* delims==" %%a in ("%WORKER_DIR%\\.env") do set "%%a=%%b"
!PY! flow_worker.py --count {accounts}

:: If worker exits, don't close so user can see errors
echo.
echo   Worker stopped. Press any key to close.
pause >nul
'''


def _generate_unix_installer(token: str, app_url: str, accounts: int = 1, reset: bool = False, update_only: bool = False) -> str:
    """Generate a Mac/Linux .command installer with minimal terminal output."""
    
    env_accounts = "ACCOUNT1_ENABLED=true"
    for n in range(2, accounts + 1):
        env_accounts += f"\nACCOUNT{n}_ENABLED=true"
    for n in range(accounts + 1, 5):
        env_accounts += f"\nACCOUNT{n}_ENABLED=false"
    
    multi = "true" if accounts > 1 else "false"
    
    # Build reset commands
    reset_cmds = ''
    if reset:
        # Wipe ALL session folders (not just up to N) — prevents auto-detection picking up leftovers
        reset_cmds = '\necho "  Resetting sessions..."\nrm -rf "$DIR/chrome-session" "$DIR/chrome-golden"'
        for n in range(2, 5):  # Always wipe all 4
            reset_cmds += f'\nrm -rf "$DIR/chrome-session-{n}" "$DIR/chrome-golden-{n}"'
        reset_cmds += '\necho "  ✓ Sessions reset"\n'
    
    # Update-only mode
    if update_only:
        return f'''#!/bin/bash
DIR="$HOME/veo-worker"
echo ""
echo "  Updating KavenoBuilder Worker..."
curl -sL "{app_url}/api/user-worker/download/flow_worker.py" -o "$DIR/flow_worker.py"
echo "  ✓ Worker updated. Restart your worker to use the new version."
echo ""
sleep 3
'''
    
    return f'''#!/bin/bash
# KavenoBuilder Worker Setup — runs silently, starts worker in background.

set -e
DIR="$HOME/veo-worker"
LOG="$DIR/setup.log"
mkdir -p "$DIR"

log() {{ echo "$(date +%H:%M:%S) $1" >> "$LOG"; }}

echo ""
echo "  ======================================================"
echo "   KavenoBuilder Worker Setup"
echo "  ======================================================"
echo ""

log "=== KavenoBuilder Worker Setup ==="

# Find Python
PY=""
for name in python3 python; do
    if command -v $name &>/dev/null; then PY=$(command -v $name); break; fi
done

if [ -z "$PY" ]; then
    echo "  Installing Python..."
    if [[ "$OSTYPE" == "darwin"* ]] && command -v brew &>/dev/null; then
        brew install python@3.12 >> "$LOG" 2>&1
    elif command -v apt-get &>/dev/null; then
        sudo apt-get update -qq >> "$LOG" 2>&1 && sudo apt-get install -y -qq python3 python3-pip >> "$LOG" 2>&1
    fi
    PY=$(command -v python3 2>/dev/null || command -v python 2>/dev/null)
fi

if [ -z "$PY" ]; then
    echo "  ERROR: Python not found. Install from https://python.org/downloads"
    read -p "  Press Enter to close..."
    exit 1
fi
log "Python: $PY"
echo "  [1/5] Python found"
{reset_cmds}
# Packages
echo "  [2/5] Installing packages..."
$PY -m pip install patchright requests --quiet 2>/dev/null || \\
$PY -m pip install patchright requests --quiet --user 2>/dev/null || \\
$PY -m pip install patchright requests --quiet --break-system-packages 2>/dev/null || true
log "Packages installed"
echo "        OK"

echo "  [3/5] Downloading worker..."
# Download worker
curl -sL "{app_url}/api/user-worker/download/flow_worker.py" -o "$DIR/flow_worker.py"
log "Worker downloaded"
echo "        OK"

# Create session folders
mkdir -p "$DIR/chrome-session" "$DIR/chrome-download"
for n in $(seq 2 {accounts}); do
    mkdir -p "$DIR/chrome-session-$n" "$DIR/chrome-download-$n"
done

echo "  [4/5] Writing config..."
# Write .env
cat > "$DIR/.env" << 'ENVEOF'
WORKER_MODE=user
USER_WORKER_TOKEN={token}
WEB_APP_URL={app_url}
SESSION_FOLDER=$HOME/veo-worker/chrome-session
DOWNLOAD_SESSION_FOLDER=$HOME/veo-worker/chrome-download
BROWSER_MODE=stealth
MULTI_ACCOUNT={multi}
MULTI_ACCOUNT_MODE={multi}
PROXY_TYPE=none
{env_accounts}
ENVEOF
# Fix $HOME in .env (heredoc doesn't expand inside single-quoted delimiter)
sed -i.bak "s|\\$HOME|$HOME|g" "$DIR/.env" 2>/dev/null || sed -i "" "s|\\$HOME|$HOME|g" "$DIR/.env"
rm -f "$DIR/.env.bak"
log ".env written"
echo "        OK"

echo "  [5/5] Creating launcher..."
# Create launcher
cat > "$DIR/start_worker.sh" << LAUNCHEOF
#!/bin/bash
cd "$DIR"
set -a; source .env; set +a
$PY flow_worker.py --count {accounts}
LAUNCHEOF
chmod +x "$DIR/start_worker.sh"

echo ""
echo "  ======================================================"
echo "   Setup complete! Starting worker..."
echo "  ======================================================"
echo ""

log "Starting worker"

# Run worker in foreground (matches Windows .bat behavior — user sees output)
cd "$DIR"
set -a; source .env; set +a
echo "  Chrome will open — click 'Continue without signing in' if prompted,"
echo "  then log into your Google account on the Flow page."
echo "  Keep this window open while the worker runs."
echo "  Check status: {app_url}/static/my-worker.html"
echo ""
$PY flow_worker.py --count {accounts}

# If worker exits, don't close so user can see errors
echo ""
echo "  Worker stopped. Press Enter to close."
read
'''


# ============ Main Entry Point ============

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host=app_config.host,
        port=app_config.port,
        reload=app_config.debug,
    )