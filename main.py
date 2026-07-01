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
from lifecycle import apply_lifecycle_change, compute_stuck_days, apply_jobs_filters, _maybe_auto_enter_lifecycle, derive_effective_stage, _LIFECYCLE_STAGE_TO_TIMESTAMP_FIELD
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
    clip_mode: Optional[str] = "fresh"     # v782 default fresh (was blend) | 'blend' | 'continue' | 'fresh'
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
    # v698A — per-scene clip-pair metadata for voiceover-over-b-roll.
    # clip_role = 'visual_pair' when the line is the visual side of a
    # paired clip (silent b-roll); the platform creates a sibling
    # audio_pair Clip from the voiceover_anchor_image at render time.
    # NULL on every non-voiceover line. voiceover_anchor_image_node_id
    # is the FK to the audio twin's start-frame ImageNode (resolved by
    # prepare_batch_for_video). voiceover_anchor_image_local_index is
    # the same image's position in the upload list (used by the worker
    # to fetch the upload). voiceover_line is the line text (mirrors
    # `text:` for clarity).
    clip_role: Optional[str] = None
    voiceover_anchor_image_node_id: Optional[int] = None
    voiceover_anchor_image_local_index: Optional[int] = None
    voiceover_line: Optional[str] = None
    # v789 — operator-authored audio-twin prompt (markdown
    # `### Clip S.L.audio` block). When set, Phase 3b uses it verbatim as
    # the audio_pair Clip's prompt_text instead of build_prompt
    # auto-construction. NULL = auto-build (pre-v789 behavior).
    voiceover_audio_prompt_override: Optional[str] = None
    # v718i (NEW 2026-05-18) — Veo native end-frame interpolation binding.
    # When set, veo_generator.py:2605 binds cfg.last_frame to this ImageNode's
    # rendered output instead of auto-inferring from next clip's start image.
    # NULL on every non-Option-C dialogue line (legacy sequential default).
    end_frame_image_node_id: Optional[int] = None
    end_frame_image_local_index: Optional[int] = None


class SceneInput(BaseModel):
    sceneIndex: int
    # v682e — imageIndex is Optional because text_card scenes have no
    # uploaded image (they render via ffmpeg drawtext at video assembly,
    # not Veo). Pre-v682e the Pydantic int requirement rejected the
    # whole job-creation request when ANY text_card scene was in the
    # storyboard, with the error:
    #   `body.scenes[N].imageIndex: Input should be a valid integer, input:null`
    imageIndex: Optional[int] = None
    clipMode: str = "fresh"        # v782 default fresh (was blend) | 'blend' | 'continue' | 'fresh'
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
    veo_model: str = "Veo 3.1 - Lite [Lower Priority]"  # Which Veo model the worker selects in Flow
    video_backend: str = ""  # "" = normal (Veo). "higgsfield" = Kling image-to-video via Higgsfield API.
    kling_variant: bool = False  # When True, server adds a Kling i2v variant to each clip (alongside Veo).
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


# JobResponse is defined in job_response.py so tests can import it without
# loading the full FastAPI application (which has startup side effects).
from job_response import JobResponse  # noqa: E402


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
    clip_mode: Optional[str] = "fresh"  # v782 default fresh (was blend)
    scene_index: Optional[int] = 0
    # Prompt
    prompt_text: Optional[str] = None
    # Lineup
    in_lineup: bool = True
    # v698A — clip-pair metadata for paired-card UI rendering. clip_role
    # NULL/single = standard clip (default). 'visual_pair' = b-roll silent
    # clip with paired audio twin. 'audio_pair' = audio source whose visual
    # is discarded; frontend filters these out of the standalone clip list.
    clip_role: Optional[str] = None
    paired_clip_id: Optional[int] = None
    voiceover_anchor_image_node_id: Optional[int] = None
    voiceover_line: Optional[str] = None
    # v718i (NEW 2026-05-18) — Veo native end-frame interpolation binding.
    end_frame_image_node_id: Optional[int] = None
    # v701 — when error_code == 'CONTENT_POLICY_VIOLATION', the previously
    # rejected start_frame R2 key is exposed here so the frontend can
    # render the offending image inside the "upload replacement" card.
    replacement_start_frame: Optional[str] = None


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
    # v75x — video stall relief. Clips stream through this Render proxy; each
    # in-flight <video> pins one anyio worker thread for its whole playback
    # (download_output reads the R2 body 1MB at a time via asyncio.to_thread).
    # The default anyio limiter is 40 threads → on the 1-CPU origin a few
    # concurrent clips + the per-request DB-auth thread + the clip poll
    # saturate it, so new chunk reads queue behind busy threads and the player
    # gets ~1s then stalls/re-buffers. These threads are IO-bound (blocked on
    # R2 network reads), NOT CPU-bound, so raising the cap is safe on 1 CPU and
    # directly relieves the saturation. Must run inside the event loop.
    try:
        import anyio
        anyio.to_thread.current_default_thread_limiter().total_tokens = 256
        print("[startup][v75x] anyio thread limiter total_tokens=256 (video stream relief)", flush=True)
    except Exception as _tle:
        print(f"[startup][v75x] thread limiter bump failed: {_tle}", flush=True)

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
        # v780 — video (Flow) worker heartbeat. POST every 5s per worker;
        # silence the request-log noise.
        "/api/user-worker/heartbeat",
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
        "/auth/me", "/api/health", "/favicon.ico",
        # v773.11.1 (2026-05-29): PostHog analytics bootstrap endpoints.
        # /api/posthog-config returns only the PUBLIC PostHog project key
        # (designed for browser exposure), and we want anonymous visitors
        # tracked too (otherwise login-page sessions never get a distinct_id).
        # /api/me returns {authenticated: false} for unauthed callers by
        # design, so exposing it to anon traffic just lets the bootstrap
        # decide whether to call posthog.identify() — no PII leaks.
        "/api/posthog-config", "/api/me",
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


# === PWA install support (2026-06-02) ===
# Serve sw.js + manifest.webmanifest at ROOT path so the service worker
# scope covers the whole site. PWA install enables persistent File System
# Access folder permissions (Chrome 121+).
@app.get("/sw.js", include_in_schema=False)
def serve_service_worker():
    from fastapi.responses import FileResponse
    return FileResponse(
        str(static_dir / "sw.js"),
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/", "Cache-Control": "no-cache"},
    )


@app.get("/manifest.webmanifest", include_in_schema=False)
def serve_pwa_manifest():
    from fastapi.responses import FileResponse
    return FileResponse(
        str(static_dir / "manifest.webmanifest"),
        media_type="application/manifest+json",
        headers={"Cache-Control": "no-cache"},
    )


# ============ Version Endpoint (for deployment verification) ============

@app.get("/api/version")
def get_version():
    """Return version info to verify which code is deployed"""
    return {
        "app": "veo-web-app",
        "worker_version": WORKER_VERSION,
        "render_commit": os.environ.get("RENDER_GIT_COMMIT", "not set"),
    }


@app.get("/api/higgsfield-ping")
def higgsfield_ping(
    slug: str = "kling-video/v2.1/pro/image-to-video",
    image_url: str = "https://picsum.photos/seed/hf/720/1280",
    current_user: User = Depends(get_current_user),
):
    """Diagnostic (auth required): raw POST to the Higgsfield REST API with the
    server HF_KEY. Returns status + body so we can find which model_id the key
    is allowed to use. A 403 burns no credits; a 200 queues a ~10-credit gen.
    Probe other models with ?slug=<model_id>."""
    import requests as _rq
    from config import get_higgsfield_credentials_from_env
    creds = get_higgsfield_credentials_from_env()
    if not creds:
        return {"ok": False, "error": "HF_KEY / HF_API_KEY+HF_API_SECRET not set on server"}
    key_shape = {"has_colon": ":" in creds, "key_id_prefix": (creds.split(":", 1)[0][:6] + "…"), "len": len(creds)}
    try:
        r = _rq.post(
            f"https://platform.higgsfield.ai/{slug}",
            headers={"Authorization": f"Key {creds}", "Content-Type": "application/json", "Accept": "application/json"},
            json={"image_url": image_url, "prompt": "subtle natural motion, static locked-off camera", "duration": 5},
            timeout=30,
        )
        return {"ok": r.status_code < 400, "slug": slug, "status": r.status_code, "body": r.text[:1500], "key_shape": key_shape}
    except Exception as e:
        return {"ok": False, "slug": slug, "error": str(e), "key_shape": key_shape}


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
    job_id: Optional[str] = None,
    limit: int = Query(default=150, ge=1, le=1000),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Backfill has_export/has_voice_clone flags by scanning R2 outputs.

    Scanning EVERY job with a synchronous R2 list each timed out the
    gateway (502) once the account accumulated 1000+ jobs. Two guards:
      - job_id=<id> scans exactly one job (instant; use to unstick a
        specific clip).
      - otherwise only jobs MISSING a flag are queried (already-flagged
        jobs need no R2 call) and the set is capped at `limit`, newest
        first, so the request stays well under the gateway timeout.
    """
    from backends.storage import is_storage_configured, get_storage

    if not is_storage_configured():
        return {"error": "Storage not configured"}

    storage = get_storage()
    if job_id:
        jobs = db.query(Job).filter(
            Job.user_id == current_user.id,
            Job.id == job_id,
        ).all()
    else:
        # Only jobs still missing a flag need an R2 scan. Cap + newest-first
        # keeps the synchronous R2 listing bounded (no 502 on large accounts).
        jobs = (
            db.query(Job)
            .filter(
                Job.user_id == current_user.id,
                Job.status.in_(['completed', 'processing']),
                (Job.has_export == False) | (Job.has_voice_clone == False),  # noqa: E712
            )
            .order_by(Job.created_at.desc())
            .limit(limit)
            .all()
        )

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
                    # v783 (2026-06-05): admin backfill also repairs the badge
                    # — final-export exists means job is done. See export-final
                    # endpoint for the prior-redo-revert rationale.
                    if job.status not in (JobStatus.CANCELLED.value, JobStatus.FAILED.value, JobStatus.COMPLETED.value):
                        job.status = JobStatus.COMPLETED.value
                        if job.completed_at is None:
                            job.completed_at = datetime.utcnow()
                    _maybe_auto_enter_lifecycle(job, now=datetime.utcnow())
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


@app.get("/auth/google/drive/connect")
async def auth_google_drive_connect(request: Request, current_user: User = Depends(get_current_user)):
    """Initiate Google Drive OAuth (separate consent from login — drive.readonly scope)."""
    from drive_auth import handle_drive_connect
    return await handle_drive_connect(request)


@app.get("/auth/google/drive/callback")
async def auth_google_drive_callback(
    request: Request,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Callback for Drive OAuth — captures refresh_token + creates DriveAccount."""
    from drive_auth import handle_drive_callback
    try:
        await handle_drive_callback(request, current_user, db)
    except HTTPException as e:
        return HTMLResponse(
            f"<html><body style='font-family:sans-serif;padding:40px;'>Drive connect failed: {e.detail}<br><a href='/'>back</a></body></html>",
            status_code=e.status_code,
        )
    # Redirect back to Browse mode, drive panel.
    return HTMLResponse(
        "<html><script>window.location.href='/?mode=browse&drive=connected';</script></html>"
    )


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
    video_backend_pref = config_dict_raw.get('video_backend', '')

    if video_backend_pref == 'higgsfield':
        # Kling (Higgsfield) toggle is ON — overrides the mode selector.
        # Clips animate via the Higgsfield API on the server-side JobWorker
        # (not Flow, not Gemini). Needs only the server HF_KEY.
        from config import get_higgsfield_credentials_from_env
        backend = BackendType.HIGGSFIELD
        if get_higgsfield_credentials_from_env():
            print(f"[main.py] Backend = HIGGSFIELD (Kling i2v toggle ON)", flush=True)
        else:
            errors.append("Kling (Higgsfield) is ON but HF_KEY is not configured on the server.")
    elif backend_preference == 'prompt_only':
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

    elif backend == BackendType.HIGGSFIELD:
        # Kling i2v via Higgsfield API — no Gemini/Flow needed, just the server HF_KEY.
        errors = [e for e in errors if "API key" not in e and "Gemini" not in e and "Flow" not in e]
        if errors:
            raise HTTPException(
                status_code=400,
                detail={"errors": errors, "code": ErrorCode.INVALID_CONFIG.value}
            )
        api_keys_data = {
            "gemini_keys": [],
            "openai_key": api_keys_config.openai_api_key
        }
        print(f"[main.py] HIGGSFIELD backend: Kling i2v, no Gemini keys needed", flush=True)

    # Create job record
    config_dict = config.model_dump()
    # Ensure the worker's VideoConfig diverts to Kling for this backend.
    if backend == BackendType.HIGGSFIELD:
        config_dict['video_backend'] = 'higgsfield'
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
        clip_mode = line.get('clip_mode', 'fresh') if isinstance(line, dict) else 'fresh'  # v782 default fresh
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

        # v698A — per-scene clip-pair metadata. clip_role='visual_pair'
        # when the line is the visual side of a voiceover-paired scene;
        # the worker render flow (Phase 3) reads this and enqueues an
        # additional audio-pair Veo job from voiceover_anchor_image.
        # NULL on every non-voiceover clip — single-render path unchanged.
        clip_role_val = line.get('clip_role') if isinstance(line, dict) else None
        voiceover_anchor_node_id = (
            line.get('voiceover_anchor_image_node_id')
            if isinstance(line, dict) else None
        )
        voiceover_line_val = (
            line.get('voiceover_line') if isinstance(line, dict) else None
        )
        # v718i (NEW 2026-05-18) — v718h-C Option C Veo native end-frame
        # interpolation per-clip binding. When the source Scene declared
        # `- **end_frame_image:** image_K+1`, the parsed ImageSceneAssignment
        # carries end_frame_image_node_id which prepare_batch_for_video
        # propagated into the line dict. veo_generator.py:2605 uses this
        # to bind cfg.last_frame (overrides sequential auto-inference from
        # next clip's start image). NULL on every non-Option-C line.
        end_frame_image_node_id_val = (
            line.get('end_frame_image_node_id')
            if isinstance(line, dict) else None
        )

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
            # v698A — voiceover-pair fields. clip_role NULL = single-render
            # path (default); 'visual_pair' = needs an audio twin.
            clip_role=clip_role_val,
            voiceover_anchor_image_node_id=voiceover_anchor_node_id,
            voiceover_line=voiceover_line_val,
            # v718i (NEW 2026-05-18) — explicit end-frame image binding for
            # Veo native end-frame interpolation. NULL = sequential auto-inference.
            end_frame_image_node_id=end_frame_image_node_id_val,
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
    
    # Kling (Higgsfield) additional-variant pass: queue each clip. The LOCAL
    # user-worker drains this queue (/api/user-worker/clips/kling-pending) using
    # the operator's authenticated `higgsfield` CLI — Kling 3.0 with audio, from
    # a residential IP. The server does NOT generate Kling itself (the official
    # key API only exposes silent Kling v2.1 + Cloudflare-blocks the v3 web API).
    if config_dict.get('kling_variant'):
        try:
            from sqlalchemy import update as _sa_update
            db.execute(
                _sa_update(Clip)
                .where(Clip.job_id == job_id)
                .values(kling_variant_status='queued')
            )
            db.commit()
            add_job_log(db, job_id, "🎬 Kling variant queued for each clip — local worker will generate (Kling 3.0 + audio)", "INFO", "kling")
        except Exception as _e:
            try:
                db.rollback()
            except Exception:
                pass
            print(f"[main.py] Could not queue Kling variants (non-fatal): {_e}", flush=True)

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
        lifecycle_stage=job.lifecycle_stage,
        approval_at=job.approval_at.isoformat() if job.approval_at else None,
        export_at=job.export_at.isoformat() if job.export_at else None,
        finishing_at=job.finishing_at.isoformat() if job.finishing_at else None,
        published_at=job.published_at.isoformat() if job.published_at else None,
        notes=job.notes,
        archived=bool(getattr(job, 'archived', False)),
        stuck_days=compute_stuck_days(job, datetime.utcnow()),
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
            # v700e — surface fast-lane gate decision so users can see WHY
            # it skipped. When the gate fails on a "should-be-prebuilt" job
            # (every scene has a `## Veo 3.1 Final Prompts` entry in the
            # source markdown but the override didn't make it through to
            # DialogueLineInput), this print shows exactly which line
            # indexes lack the override and what their scene_type / clip_role
            # / dialogue snippet are. Pre-v700e the only way to debug a
            # gate miss was to dump dialogue_json from psql.
            if not _all_prebuilt and bool(_peek_lines):
                _missing = [
                    {
                        "i": _i,
                        "scene_type": (_l.get("scene_type") if isinstance(_l, dict) else None),
                        "clip_role": (_l.get("clip_role") if isinstance(_l, dict) else None),
                        "scene_index": (_l.get("scene_index") if isinstance(_l, dict) else None),
                        "text_head": ((_l.get("text") or "")[:40] if isinstance(_l, dict) else ""),
                        "has_text_prompt": bool((_l.get("veo_prompt_override") or "").strip()) if isinstance(_l, dict) else False,
                    }
                    for _i, _l in enumerate(_peek_lines)
                    if not _line_satisfies_fast_lane(_l)
                ]
                print(
                    f"[Background] v673 fast lane SKIPPED for job {job_id[:8]}... — "
                    f"{len(_missing)}/{len(_peek_lines)} line(s) missing veo_prompt_override:",
                    flush=True,
                )
                for _m in _missing:
                    print(
                        f"  [v700e] line {_m['i']} scene={_m['scene_index']} "
                        f"role={_m['clip_role']!r} type={_m['scene_type']!r} "
                        f"has_prompt={_m['has_text_prompt']} text={_m['text_head']!r}",
                        flush=True,
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
                clip_mode = line_data.get("clip_mode", "fresh")  # v782 default fresh (was blend) — no silent self/cross-scene start/end interpolation
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

                # v718i.3 (NEW 2026-05-18 late) — EXPLICIT END-FRAME OVERRIDE.
                # When line_data carries end_frame_image_local_index (set by
                # v718i frontend → DialogueLineInput → here), the operator has
                # declared an explicit Veo native end-frame interpolation
                # binding via `- **end_frame_image:** image_K+1` in the Scene
                # block. Resolve to the uploaded frames list and skip the
                # legacy scene-transition / blend / last-clip fallback below.
                # Without this override the Flow path's Clip.end_frame stays
                # NULL → flow_worker uploads only start_frame → Veo gets
                # cfg.image but NO cfg.last_frame → no native morphological
                # interpolation. Worker.py already reads
                # explicit_end_frame_local_index for the Vertex path but the
                # Flow path (kavenobuilder.com production) goes through
                # main.py background prompt-build → Clip.end_frame → R2 key
                # → flow_worker.py upload. v718i.3 closes the Flow-path gap.
                end_fname = None
                _explicit_end_idx_v718i = line_data.get("end_frame_image_local_index")
                if (
                    _explicit_end_idx_v718i is not None
                    and isinstance(_explicit_end_idx_v718i, int)
                    and 0 <= _explicit_end_idx_v718i < num_images
                ):
                    end_fname = uploaded_frames_list[_explicit_end_idx_v718i]
                    print(
                        f"[v718i.3] Clip {idx}: explicit end-frame override "
                        f"(Option C native interpolation) → image_local_index="
                        f"{_explicit_end_idx_v718i} → end_fname={end_fname}",
                        flush=True,
                    )

                is_last_clip = (idx == total_clips - 1)
                scenes = dialogue_data.get("scenes", [])

                # Skip the legacy fallback when v718i.3 already resolved
                # end_fname (operator's explicit binding wins over inferred).
                if end_fname is None and scenes:
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
                                if next_scene.get("transition", "cut") != "cut":  # v782 default cut (was blend) — missing transition no longer triggers cross-scene end-frame interpolation
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
                elif end_fname is None and num_images == 1 and use_interpolation and clip_mode == "blend":
                    end_fname = start_fname
                elif end_fname is None and num_images > 1 and clip_mode == "blend" and use_interpolation:
                    # Multi-image, blend mode, no scenes → self-interpolation
                    end_fname = start_fname

                # v782 DIAGNOSTIC (temporary — remove after operator confirms no unwanted blends):
                # log the resolved clip_mode + whether an end frame (interpolation/blend)
                # was assigned for this clip. With v782 defaults (clip_mode=fresh,
                # transition=cut), end_fname should be None unless the build EXPLICITLY
                # set clip_mode: blend or an end_frame_image. A non-None end_fname on a
                # fresh/cut clip means an unwanted blend slipped through.
                print(
                    f"[v782] Clip {idx}: clip_mode={clip_mode!r} end_fname={end_fname!r} "
                    f"(end_frame {'ASSIGNED' if end_fname else 'none'})",
                    flush=True,
                )

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

                # v698A Phase 3c — visual_pair clips render SILENT visuals
                # (b-roll, no lip-sync). Override the dialogue + force
                # voiceover_only=False so build_prompt produces a silent
                # b-roll prompt; the audio twin (Phase 3a/3b audio_pair Clip)
                # carries the line on the anchor frame separately. The
                # visual prompt's action_note still drives the b-roll
                # motion arc per v697.
                _line_clip_role = (
                    line_data.get("clip_role")
                    if isinstance(line_data, dict) else None
                )
                if (_line_clip_role or "").lower() == "visual_pair":
                    _padded_dialogue_for_veo = ""  # silent visual
                    _voiceover_only_override = False  # not narrated either
                    print(
                        f"[v698A/Phase3c] clip {idx} clip_role=visual_pair → "
                        f"silent prompt (b-roll, no lip-sync). Audio twin "
                        f"renders the voiceover separately.",
                        flush=True,
                    )

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

            # v698A Phase 3a — audio_pair Clip row creation for voiceover-paired
            # scenes. After the main prompt-build loop completes, every clip
            # with clip_role='visual_pair' gets a sibling audio_pair Clip
            # created. The audio_pair carries the voiceover_line as
            # dialogue_text and references the voiceover_anchor_image as its
            # start frame. paired_clip_id is set bidirectionally so Phase 3c's
            # render dispatch can resolve the pair atomically.
            #
            # Phase 3a behavior: audio_pair Clips are created at status
            # 'preparing' (NOT pending). Phase 3b will build their prompts
            # using the anchor image; Phase 3c will mark them pending +
            # dispatch render; Phase 3d adds atomic completion gating. Until
            # Phase 3b lands, audio_pair Clips sit dormant — visible in DB,
            # not yet rendered, no UI surfacing.
            try:
                visual_pair_clips = db.query(Clip).filter(
                    Clip.job_id == job_id,
                    Clip.clip_role == 'visual_pair',
                ).all()
                if visual_pair_clips:
                    # Audio-pair Clips use clip_index offset so they sort cleanly
                    # in DB queries while not colliding with natural clip_index
                    # values (which are 0..N-1 for the lineup). Convention:
                    # audio_pair.clip_index = visual_pair.clip_index + 100000
                    audio_pair_offset = 100000
                    audio_pairs_created = 0
                    for vp in visual_pair_clips:
                        # Skip if already paired (idempotent — re-runs of
                        # _setup_job_background after a partial failure)
                        if vp.paired_clip_id is not None:
                            continue
                        if not vp.voiceover_line:
                            print(
                                f"[v698A/Phase3a] visual_pair clip {vp.id} "
                                f"missing voiceover_line — skipping audio twin",
                                flush=True,
                            )
                            continue
                        if not vp.voiceover_anchor_image_node_id:
                            print(
                                f"[v698A/Phase3a] visual_pair clip {vp.id} "
                                f"missing voiceover_anchor_image_node_id — skipping",
                                flush=True,
                            )
                            continue

                        ap = Clip(
                            job_id=job_id,
                            clip_index=audio_pair_offset + vp.clip_index,
                            dialogue_id=vp.dialogue_id,
                            dialogue_text=vp.voiceover_line,
                            status='preparing',  # Phase 3b → 'pending' after prompt build
                            scene_index=vp.scene_index,
                            clip_role='audio_pair',
                            paired_clip_id=vp.id,
                            # Anchor image FK denormed from the visual_pair so
                            # the worker render dispatch (Phase 3c) can pick it
                            # up without re-resolving via dialogue_json.
                            voiceover_anchor_image_node_id=vp.voiceover_anchor_image_node_id,
                            voiceover_line=vp.voiceover_line,
                            # cut_mode='auto' on audio_pair — its Whisper-VAD
                            # runs at export time on the LINE script.
                            cut_mode='auto',
                            # text_card / caption / bg_color — all NULL on
                            # audio_pair (visual is discarded at export anyway).
                            scene_type='shot',
                        )
                        db.add(ap)
                        db.flush()  # populate ap.id

                        # Bidirectional link
                        vp.paired_clip_id = ap.id
                        audio_pairs_created += 1

                    if audio_pairs_created:
                        db.commit()
                        print(
                            f"[v698A/Phase3a] created {audio_pairs_created} "
                            f"audio_pair Clip rows for {len(visual_pair_clips)} "
                            f"visual_pair Clip rows",
                            flush=True,
                        )
                        add_job_log(
                            db, job_id,
                            f"v698A: {audio_pairs_created} audio twins created "
                            f"for voiceover-paired scenes",
                            "INFO", "system",
                        )

                    # v698A Phase 3b — build prompts + set start_frame on
                    # audio_pair Clips so Flow worker can dispatch them.
                    # The audio_pair Clip's start frame is the
                    # voiceover_anchor_image (resolved via
                    # voiceover_anchor_image_local_index from the line metadata).
                    # build_prompt is called with speaker_mode='on-camera' so
                    # Veo's lip-sync runs normally; the line text is the
                    # voiceover_line.
                    audio_pair_clips = db.query(Clip).filter(
                        Clip.job_id == job_id,
                        Clip.clip_role == 'audio_pair',
                        Clip.status == 'preparing',
                    ).all()
                    audio_prompts_built = 0
                    for ap in audio_pair_clips:
                        try:
                            # Find visual_pair sibling to get the dialogue_raw
                            # line (where voiceover_anchor_image_local_index lives)
                            vp = db.query(Clip).filter(Clip.id == ap.paired_clip_id).first()
                            if vp is None:
                                print(
                                    f"[v698A/Phase3b] audio_pair {ap.id} has "
                                    f"no paired visual; skipping",
                                    flush=True,
                                )
                                continue
                            vp_idx = vp.clip_index
                            if vp_idx >= len(dialogue_raw):
                                continue
                            line_data = (
                                dialogue_raw[vp_idx]
                                if isinstance(dialogue_raw[vp_idx], dict)
                                else {}
                            )
                            anchor_local_idx = line_data.get(
                                "voiceover_anchor_image_local_index"
                            )
                            if anchor_local_idx is None:
                                print(
                                    f"[v698A/Phase3b] audio_pair {ap.id} missing "
                                    f"anchor_local_idx in line_data; skipping",
                                    flush=True,
                                )
                                continue
                            if anchor_local_idx >= len(uploaded_frames_list):
                                print(
                                    f"[v698A/Phase3b] audio_pair {ap.id} "
                                    f"anchor_local_idx={anchor_local_idx} out of range "
                                    f"(uploaded_frames_list len={len(uploaded_frames_list)})",
                                    flush=True,
                                )
                                continue

                            anchor_fname = uploaded_frames_list[anchor_local_idx]
                            anchor_local_path = local_frame_paths.get(anchor_fname)
                            if anchor_local_path is None or not os.path.exists(anchor_local_path):
                                print(
                                    f"[v698A/Phase3b] audio_pair {ap.id} anchor "
                                    f"local file missing ({anchor_fname}); skipping",
                                    flush=True,
                                )
                                continue

                            # v789 — operator-authored audio-twin prompt
                            # (markdown `### Clip S.L.audio` block, plumbed
                            # through the dialogue payload). When present it
                            # is used VERBATIM as the audio_pair prompt;
                            # build_prompt auto-construction only fires as
                            # the fallback.
                            _authored_audio = (
                                (line_data.get("voiceover_audio_prompt_override") or "").strip()
                                or None
                            )
                            if _authored_audio:
                                ap.prompt_text = _authored_audio
                                ap.start_frame = (
                                    f"jobs/{job_id}/frames/{anchor_fname}"
                                )
                                ap.status = ClipStatus.PENDING.value
                                audio_prompts_built += 1
                                print(
                                    f"[v789] audio_pair {ap.id} using AUTHORED "
                                    f"twin prompt ({len(_authored_audio)} chars) "
                                    f"— build_prompt skipped",
                                    flush=True,
                                )
                                continue

                            # Build the audio_pair Veo prompt — speaker_mode=
                            # 'on-camera' so Veo lip-syncs the line on the
                            # torso+hands anchor frame.
                            try:
                                audio_prompt = await asyncio.to_thread(
                                    build_prompt,
                                    dialogue_line=ap.voiceover_line or vp.voiceover_line or "",
                                    start_frame_path=anchor_local_path,
                                    end_frame_path=None,
                                    clip_index=ap.clip_index,
                                    language=language,
                                    voice_profile=voice_profile,
                                    config=video_config,
                                    openai_key=openai_key,
                                    frame_analysis=frame_analysis,
                                    user_context_override=(
                                        user_context_enriched if user_context_enriched else None
                                    ),
                                    use_gesture_enrichment=False,
                                    transition_cue=None,
                                    action_note=None,
                                    short_dialogue_mode=config_dict.get(
                                        'short_dialogue_mode', 'optimized'
                                    ),
                                    voiceover_only=False,  # on-camera lip-sync
                                )
                            except Exception as _bp_err:
                                print(
                                    f"[v698A/Phase3b] build_prompt failed for "
                                    f"audio_pair {ap.id}: {_bp_err}",
                                    flush=True,
                                )
                                continue

                            ap.prompt_text = audio_prompt
                            ap.start_frame = (
                                f"jobs/{job_id}/frames/{anchor_fname}"
                            )
                            ap.status = ClipStatus.PENDING.value
                            audio_prompts_built += 1
                        except Exception as _ap_err:
                            print(
                                f"[v698A/Phase3b] audio_pair {ap.id} prompt "
                                f"build failed (non-fatal): {_ap_err}",
                                flush=True,
                            )
                    if audio_prompts_built:
                        db.commit()
                        print(
                            f"[v698A/Phase3b] built prompts + set "
                            f"start_frame on {audio_prompts_built} audio_pair "
                            f"Clips → status=pending (Flow worker pickup ready)",
                            flush=True,
                        )
            except Exception as _vp_err:
                # Non-fatal — visual side will still render; audio twin can be
                # created later via redo or a follow-up sweep
                print(
                    f"[v698A/Phase3a] audio_pair creation failed (non-fatal): "
                    f"{_vp_err}",
                    flush=True,
                )
                import traceback as _tb
                _tb.print_exc()

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

def _build_job_response(job, first_dialogue=None, first_frame_url=None, approved_clips=0):
    """Shared JobResponse serializer. Used by list_jobs, get_job, patch_lifecycle, patch_archive.

    The returned `lifecycle_stage` is the EFFECTIVE stage derived from has_export
    + approved_clips count (see lifecycle.derive_effective_stage). Manual
    terminal stages (PUBLISHED, AWAITING_FINISHING post-export) stick.
    `approved_clips` MUST be passed by the caller — either from a batch
    GROUP BY query in list_jobs or via .count() in get_job.
    """
    effective_stage = derive_effective_stage(job, approved_clips)
    # Pick the most relevant timestamp for stuck_days based on derived stage.
    stuck_now = datetime.utcnow()
    if effective_stage:
        ts_field_map = {
            "awaiting_approval":  job.approval_at or job.completed_at,
            "awaiting_export":    job.export_at or job.approval_at or job.completed_at,
            "awaiting_finishing": job.finishing_at or job.export_at or job.approval_at or job.completed_at,
            "published":          job.published_at or job.finishing_at or job.completed_at,
        }
        ts = ts_field_map.get(effective_stage)
        stuck_days = (stuck_now - ts).days if ts else None
    else:
        stuck_days = None
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
        first_dialogue=first_dialogue,
        first_frame_url=first_frame_url,
        has_export=bool(getattr(job, 'has_export', False)),
        has_voice_clone=bool(getattr(job, 'has_voice_clone', False)),
        lifecycle_stage=effective_stage,
        approval_at=job.approval_at.isoformat() if job.approval_at else None,
        export_at=job.export_at.isoformat() if job.export_at else None,
        finishing_at=job.finishing_at.isoformat() if job.finishing_at else None,
        published_at=job.published_at.isoformat() if job.published_at else None,
        notes=job.notes,
        archived=bool(getattr(job, 'archived', False)),
        stuck_days=stuck_days,
        approved_clips=approved_clips,
        instagram_url=getattr(job, 'instagram_url', None),
    )


# =============================================================================
# Product analytics (PostHog) — config + identity endpoints
# =============================================================================
# v773.11.0 (2026-05-29): added PostHog integration for product analytics.
# - GET /api/posthog-config: public endpoint, returns project key + host so
#   the static HTML bootstrap can init PostHog. Key is the PUBLIC project
#   API key (designed for client-side use). Returns {enabled: false} when
#   POSTHOG_KEY env is unset so the bootstrap script no-ops gracefully.
# - GET /api/me: returns the current user's id + email so the bootstrap can
#   call posthog.identify(). Always 200; returns {authenticated: false}
#   when no session cookie present (lets the bootstrap stay anon-tracking).
@app.get("/api/posthog-config")
async def posthog_config():
    key = os.environ.get("POSTHOG_KEY", "").strip()
    host = os.environ.get("POSTHOG_HOST", "https://us.i.posthog.com").strip()
    if not key:
        return {"enabled": False}
    return {"enabled": True, "key": key, "host": host}


@app.get("/api/me")
async def whoami(
    current_user: Optional[User] = Depends(get_optional_user),
):
    if current_user is None:
        return {"authenticated": False}
    return {
        "authenticated": True,
        "id": current_user.id,
        "email": current_user.email,
    }


@app.get("/api/jobs", response_model=List[JobResponse])
async def list_jobs(
    request: Request,
    status: Optional[str] = None,
    lifecycle: Optional[str] = None,
    archived: bool = False,
    limit: int = Query(default=50, le=2000),
    offset: int = 0,
    since_days: int = Query(default=3, ge=0, le=3650),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """List jobs for the current user.

    v726 — ``since_days`` defaults to 3, restricting the result set to jobs
    created in the last N days. ``since_days=0`` disables the filter (used
    by the "Show older" UI escalation: 3 → 14 → 90 → 0).

    v728 — ``limit`` ceiling raised 100 → 2000 so the Show-older UI can
    surface older rows that the prior cap was hiding. The frontend scales
    limit alongside the window (3d → 50, 14d → 300, 90d → 1000, all → 2000)
    so payload size stays bounded.

    lifecycle — filter by lifecycle_stage value; "any" = has any stage;
    "null" = no stage assigned.
    archived — when True, return only archived jobs.
    """
    query = db.query(Job)
    query = apply_jobs_filters(
        query,
        user_id=current_user.id,
        status=status,
        since_days=since_days,
        lifecycle=lifecycle,
        archived=archived,
    )

    jobs = query.order_by(Job.created_at.desc()).offset(offset).limit(limit).all()

    # v783 (2026-06-05): status reconciler — any job where the counters
    # already say done but status is stuck mid-flight gets quietly flipped
    # to 'completed' before serialization. Two recurrent ways to land in
    # this state:
    #   1. Flow redo at main.py:12816 reverts status='processing' on a
    #      previously-completed job; the redo recompute inflates
    #      completed_clips via audio_pair/Kling-variant siblings; the
    #      'completed >= total' auto-flip in /api/user-worker/clips/{id}/
    #      status only fires when THAT specific endpoint is the path
    #      reporting the redo result. Other report paths (legacy
    #      flow-worker, abandoned redo, manual variant upload) skip it →
    #      status stuck at 'processing' indefinitely.
    #   2. Pre-fix jobs sitting at status='processing' but counters say
    #      done from earlier inflation.
    # Skip cancelled / failed (operator terminal intent). Idempotent.
    _reconciled = 0
    for j in jobs:
        if (
            j.status not in (JobStatus.COMPLETED.value, JobStatus.CANCELLED.value, JobStatus.FAILED.value)
            and (j.total_clips or 0) > 0
            and (j.completed_clips or 0) >= j.total_clips
        ):
            j.status = JobStatus.COMPLETED.value
            if j.completed_at is None:
                j.completed_at = datetime.utcnow()
            _reconciled += 1
    if _reconciled:
        db.commit()
        print(f"[jobs-list] status reconciler flipped {_reconciled} stuck job(s) → completed", flush=True)

    # Batch-fetch first clip (clip_index=0) for each job to get dialogue + frame
    job_ids = [j.id for j in jobs]
    first_clips = {}
    approved_counts = {}
    if job_ids:
        from sqlalchemy import and_, func
        clips = db.query(Clip).filter(
            and_(Clip.job_id.in_(job_ids), Clip.clip_index == 0)
        ).all()
        for c in clips:
            first_clips[c.job_id] = c
        # v776.2: batch-fetch per-job approved-clip counts so the lifecycle
        # serializer can live-derive the effective stage.
        rows = (
            db.query(Clip.job_id, func.count(Clip.id))
            .filter(and_(Clip.job_id.in_(job_ids), Clip.approval_status == "approved"))
            .group_by(Clip.job_id)
            .all()
        )
        approved_counts = {jid: n for jid, n in rows}

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

        result.append(_build_job_response(
            j,
            first_dialogue=first_dialogue,
            first_frame_url=first_frame_url,
            approved_clips=approved_counts.get(j.id, 0),
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


def _count_approved_clips(db, job_id: str) -> int:
    """Single-query approved-clip count for one job. Used by get_job and the
    PATCH endpoints so the derived lifecycle_stage is accurate per request."""
    return db.query(Clip).filter(
        Clip.job_id == job_id, Clip.approval_status == "approved"
    ).count()


@app.get("/api/jobs/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Get job details"""
    job = get_user_job(db, job_id, current_user)
    resp = _build_job_response(job, approved_clips=_count_approved_clips(db, job_id))
    # v780 — surface the source image batch (if this video job was promoted from
    # an image job) so the UI can offer a "go to image job" button. Single-job
    # GET only — kept out of the shared serializer to avoid an N+1 in list_jobs.
    try:
        from image_platform import ImageJobBatch
        _b = (
            db.query(ImageJobBatch.id)
            .filter(ImageJobBatch.promoted_video_job_id == job_id)
            .first()
        )
        if _b:
            resp.source_image_batch_id = _b[0]
            print(f"[v780] job {job_id[:8]} promoted from image batch {str(_b[0])[:8]} (Image-job button shown)", flush=True)
    except Exception as _e:
        print(f"[v780] source_image_batch_id lookup skipped (non-fatal): {_e}", flush=True)
    return resp


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


class UpdateLifecycleRequest(BaseModel):
    """PATCH /api/jobs/{id}/lifecycle body.

    All fields optional. Sending only `notes` updates notes without changing
    stage. Sending `stage=None` with `clear=True` removes the Job from the
    tracker (rare; used for test renders).
    """
    stage: Optional[str] = None
    notes: Optional[str] = None
    clear: bool = False


class UpdateArchiveRequest(BaseModel):
    archived: bool


# === Instagram monitor (2026-05-31) ===
from encryption import encrypt as _enc_encrypt, decrypt as _enc_decrypt
from instagram_client import (
    resolve_user_id as _ig_resolve_user_id,
    fetch_recent_clips as _ig_fetch_recent_clips,
    HikerAPIError,
)
import instagram_match as _ig_match


class CreateInstagramAccountRequest(BaseModel):
    handle: str
    api_key: str


class MatchInstagramVideoRequest(BaseModel):
    job_id: str


def _get_user_ig_account(db: DBSession, account_id: int, user: User):
    from models import InstagramAccount
    acc = db.query(InstagramAccount).filter_by(id=account_id).first()
    if not acc:
        raise HTTPException(status_code=404, detail="Instagram account not found")
    if acc.user_id != user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    return acc


def _job_full_dialogue(db: DBSession, job_id: str) -> str:
    """Concat each clip's SPOKEN line for a Job, in clip_index order.

    v698A b-roll voiceover scenes store the spoken words by clip_role
    (canonical rule at the broll export, ~L8095):
      - single      → spoken text in dialogue_text
      - visual_pair → spoken text in voiceover_line (dialogue_text is the
                      EMPTY on-camera text — nobody speaks on camera)
      - audio_pair  → silent render twin; dialogue_text duplicates the
                      sibling's voiceover_line (clip_index = visual+100000)

    So reconstruct with COALESCE(voiceover_line, dialogue_text) and keep
    audio_pair excluded. Reading dialogue_text alone rebuilt near-EMPTY
    text for b-roll-heavy jobs (visual_pair dialogue_text is blank),
    making IG-transcript matches near-random and surfacing wildly wrong
    suggestions. Excluding audio_pair avoids double-counting each line.
    """
    rows = (
        db.query(Clip.dialogue_text, Clip.voiceover_line)
        .filter(Clip.job_id == job_id)
        .filter((Clip.clip_role == None) | (Clip.clip_role != 'audio_pair'))  # noqa: E711
        .order_by(Clip.clip_index.asc())
        .all()
    )
    return " ".join(((vo or dt) or "").strip() for dt, vo in rows).strip()


@app.post("/api/instagram/accounts")
async def create_instagram_account(
    req: CreateInstagramAccountRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    from models import InstagramAccount
    handle = (req.handle or "").strip().lstrip("@").lower()
    if not handle or not all(c.isalnum() or c in "._" for c in handle):
        raise HTTPException(400, detail="Invalid handle")
    existing = db.query(InstagramAccount).filter_by(user_id=current_user.id, handle=handle).first()
    if existing:
        raise HTTPException(409, detail="Handle already linked")
    acc = InstagramAccount(
        user_id=current_user.id,
        handle=handle,
        api_key_encrypted=_enc_encrypt(req.api_key.strip()),
    )
    db.add(acc)
    db.commit()
    db.refresh(acc)
    return acc.to_dict()


@app.get("/api/instagram/accounts")
async def list_instagram_accounts(
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    from models import InstagramAccount, InstagramVideo
    rows = db.query(InstagramAccount).filter_by(user_id=current_user.id).order_by(InstagramAccount.added_at.desc()).all()
    out = []
    for acc in rows:
        video_count = db.query(InstagramVideo).filter_by(account_id=acc.id).count()
        matched_count = db.query(InstagramVideo).filter(
            InstagramVideo.account_id == acc.id,
            InstagramVideo.matched_job_id.isnot(None),
        ).count()
        d = acc.to_dict()
        d["video_count"] = video_count
        d["matched_count"] = matched_count
        out.append(d)
    return out


@app.delete("/api/instagram/accounts/{account_id}")
async def delete_instagram_account(
    account_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    acc = _get_user_ig_account(db, account_id, current_user)
    db.delete(acc)
    db.commit()
    return {"deleted": account_id}


@app.post("/api/instagram/accounts/{account_id}/sync")
async def sync_instagram_account(
    account_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    from models import InstagramAccount, InstagramVideo
    acc = _get_user_ig_account(db, account_id, current_user)
    api_key = _enc_decrypt(acc.api_key_encrypted)
    try:
        if not acc.ig_user_id:
            acc.ig_user_id = _ig_resolve_user_id(acc.handle, api_key)
        # limit=0 → fetch all reels via cursor pagination (max 50 pages).
        clips = _ig_fetch_recent_clips(acc.ig_user_id, api_key, limit=0)
    except HikerAPIError as he:
        raise HTTPException(status_code=502, detail=str(he))
    added = 0
    for c in clips:
        if not c.get("shortcode"):
            continue
        existing = db.query(InstagramVideo).filter_by(account_id=acc.id, shortcode=c["shortcode"]).first()
        if existing:
            existing.views = c.get("views") or 0
            existing.likes = c.get("likes") or 0
            existing.comments = c.get("comments") or 0
            # Refresh signed URLs (they expire) so retries can re-download.
            if c.get("video_url"):
                existing.video_url = c.get("video_url")
            if c.get("thumb_url"):
                existing.thumb_url = c.get("thumb_url")
            continue
        v = InstagramVideo(
            account_id=acc.id,
            shortcode=c["shortcode"],
            url=c.get("url") or f"https://www.instagram.com/reel/{c['shortcode']}/",
            thumb_url=c.get("thumb_url"),
            video_url=c.get("video_url"),
            caption=c.get("caption"),
            views=c.get("views") or 0,
            likes=c.get("likes") or 0,
            comments=c.get("comments") or 0,
            posted_at=c.get("posted_at"),
        )
        db.add(v)
        added += 1
    acc.last_synced_at = datetime.utcnow()
    db.commit()
    total = db.query(InstagramVideo).filter_by(account_id=acc.id).count()
    return {"added": added, "total": total}


@app.get("/api/instagram/accounts/{account_id}/videos")
async def list_instagram_videos(
    account_id: int,
    matched: Optional[bool] = None,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    from models import InstagramVideo
    _get_user_ig_account(db, account_id, current_user)
    q = db.query(InstagramVideo).filter_by(account_id=account_id)
    if matched is True:
        q = q.filter(InstagramVideo.matched_job_id.isnot(None))
    elif matched is False:
        q = q.filter(InstagramVideo.matched_job_id.is_(None))
    videos = q.order_by(InstagramVideo.posted_at.desc().nullslast()).all()
    return [v.to_dict() for v in videos]


@app.post("/api/instagram/videos/{video_id}/transcribe")
async def retry_transcribe_video(
    video_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    from models import InstagramVideo, InstagramAccount
    v = db.query(InstagramVideo).filter_by(id=video_id).first()
    if not v:
        raise HTTPException(404, detail="video not found")
    acc = db.query(InstagramAccount).filter_by(id=v.account_id).first()
    if not acc or acc.user_id != current_user.id:
        raise HTTPException(403, detail="access denied")
    v.transcription_status = "pending"
    v.transcription_error = None
    db.commit()
    return {"status": "pending"}


@app.post("/api/instagram/accounts/{account_id}/retry-failed")
async def retry_failed_for_account(
    account_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Reset every `failed` video on this account back to `pending` so the
    worker picks them up on the next tick."""
    from models import InstagramVideo
    _get_user_ig_account(db, account_id, current_user)
    rows = db.query(InstagramVideo).filter(
        InstagramVideo.account_id == account_id,
        InstagramVideo.transcription_status == "failed",
    ).all()
    n = 0
    for v in rows:
        v.transcription_status = "pending"
        v.transcription_error = None
        n += 1
    db.commit()
    return {"requeued": n}


@app.get("/api/instagram/videos/{video_id}/suggestions")
async def suggest_matches(
    video_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    from models import InstagramVideo, InstagramAccount, Job
    v = db.query(InstagramVideo).filter_by(id=video_id).first()
    if not v:
        raise HTTPException(404, detail="video not found")
    acc = db.query(InstagramAccount).filter_by(id=v.account_id).first()
    if not acc or acc.user_id != current_user.id:
        raise HTTPException(403, detail="access denied")
    if not v.transcription or v.transcription_status != "done":
        return []
    # Candidate pool = any COMPLETED, unlinked, non-archived job. Do NOT
    # gate on lifecycle_stage: a posted IG video's source job is always
    # fully rendered + exported, but the stored lifecycle_stage column is
    # derived live and persisted lazily — b-roll/twin jobs routinely sit
    # at awaiting_export with a stale stored stage, so a stage filter
    # silently drops the correct job and suggestions land far off.
    # status=='completed' + instagram_video_id IS NULL is the durable
    # signal that survives every stage-progression quirk.
    candidates = (
        db.query(Job)
        .filter(
            Job.user_id == current_user.id,
            Job.status == "completed",
            Job.instagram_video_id.is_(None),
            Job.archived == False,  # noqa: E712
        )
        .all()
    )
    full_dialogue = lambda j: _job_full_dialogue(db, j.id)
    # TEMP DIAGNOSTIC (ig-suggest): dump the exact two strings being
    # compared so we can see WHY suggestions land far off. Remove once
    # the b-roll match root cause is confirmed from operator logs.
    _t = v.transcription or ""
    print(f"[ig-suggest] video={video_id} transcript_status={v.transcription_status} "
          f"transcript_len={len(_t)} transcript_head={_t[:200]!r}", flush=True)
    print(f"[ig-suggest] candidate_pool={len(candidates)} "
          f"(awaiting_finishing + unlinked)", flush=True)
    for _j in candidates:
        _dlg = full_dialogue(_j)
        _s = _ig_match.score(_t, _dlg)
        print(f"[ig-suggest]   job={_j.id[:8]} score={_s:.4f} "
              f"dlg_len={len(_dlg)} dlg_head={_dlg[:160]!r}", flush=True)
    # min_score=0.0: return top 5 regardless of score. UI displays the
    # percentage so operator can pick even low-confidence matches.
    # Auto-match floor (IG_AUTO_MATCH_THRESHOLD, default 0.70) stays
    # higher so nothing gets silently linked. Empty response = no
    # candidates in pool (every awaiting_finishing job already matched).
    top = _ig_match.best_matches(v, candidates, full_dialogue=full_dialogue, k=5, min_score=0.0)
    for entry in top:
        clip = db.query(Clip).filter(Clip.job_id == entry["job_id"], Clip.clip_index == 0).first()
        entry["slug"] = (clip.dialogue_text or "")[:80] if clip and clip.dialogue_text else entry["job_id"][:8]
    return top


@app.post("/api/instagram/videos/{video_id}/match")
async def match_video(
    video_id: int,
    req: MatchInstagramVideoRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    from models import InstagramVideo, InstagramAccount, Job
    v = db.query(InstagramVideo).filter_by(id=video_id).first()
    if not v:
        raise HTTPException(404, detail="video not found")
    acc = db.query(InstagramAccount).filter_by(id=v.account_id).first()
    if not acc or acc.user_id != current_user.id:
        raise HTTPException(403, detail="access denied")
    job = db.query(Job).filter_by(id=req.job_id).first()
    if not job or job.user_id != current_user.id:
        raise HTTPException(404, detail="job not found")
    v.matched_job_id = job.id
    v.matched_at = datetime.utcnow()
    job.instagram_url = v.url
    job.instagram_video_id = v.id
    job.lifecycle_stage = "published"
    if job.published_at is None:
        job.published_at = datetime.utcnow()
    db.commit()
    return _build_job_response(job, approved_clips=_count_approved_clips(db, job.id))


@app.delete("/api/instagram/videos/{video_id}/match")
async def unmatch_video(
    video_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    from models import InstagramVideo, InstagramAccount, Job
    v = db.query(InstagramVideo).filter_by(id=video_id).first()
    if not v:
        raise HTTPException(404, detail="video not found")
    acc = db.query(InstagramAccount).filter_by(id=v.account_id).first()
    if not acc or acc.user_id != current_user.id:
        raise HTTPException(403, detail="access denied")
    matched_job_id = v.matched_job_id
    v.matched_job_id = None
    v.matched_at = None
    if matched_job_id:
        job = db.query(Job).filter_by(id=matched_job_id).first()
        if job:
            job.instagram_url = None
            job.instagram_video_id = None
    db.commit()
    return {"unmatched": video_id}


# ============================================================================
# Google Drive folder watcher (2026-06-01)
# Operator drops the final-cut video into a watched Drive folder. Backend
# polls, transcribes, matches against awaiting_finishing jobs, advances
# matched jobs to `published` with published_via='drive_watch'. Later when
# IG sync sees the actual reel, it back-fills instagram_url on the
# already-published job (per IG candidate-filter widening, Slice 3).
# ============================================================================

class SetDriveFolderRequest(BaseModel):
    folder_id: str
    folder_name: Optional[str] = None


@app.get("/api/drive/accounts")
async def list_drive_accounts(
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    from models import DriveAccount
    accs = (
        db.query(DriveAccount)
        .filter(DriveAccount.user_id == current_user.id)
        .order_by(DriveAccount.added_at.desc())
        .all()
    )
    return [a.to_dict() for a in accs]


@app.delete("/api/drive/accounts/{account_id}")
async def delete_drive_account(
    account_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    from models import DriveAccount
    acc = db.query(DriveAccount).filter_by(id=account_id).first()
    if not acc:
        raise HTTPException(404, detail="drive account not found")
    if acc.user_id != current_user.id:
        raise HTTPException(403, detail="access denied")
    db.delete(acc)
    db.commit()
    return {"deleted": account_id}


@app.post("/api/drive/accounts/{account_id}/folder")
async def set_drive_folder(
    account_id: int,
    req: SetDriveFolderRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Set the Drive folder to watch on this DriveAccount."""
    from models import DriveAccount
    acc = db.query(DriveAccount).filter_by(id=account_id).first()
    if not acc:
        raise HTTPException(404, detail="drive account not found")
    if acc.user_id != current_user.id:
        raise HTTPException(403, detail="access denied")
    folder_id = (req.folder_id or "").strip()
    if not folder_id:
        raise HTTPException(400, detail="folder_id required")
    acc.folder_id = folder_id
    acc.folder_name = (req.folder_name or "").strip() or None
    db.commit()
    db.refresh(acc)
    return acc.to_dict()


@app.get("/api/drive/accounts/{account_id}/videos")
async def list_drive_videos(
    account_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    from models import DriveAccount, DriveVideo
    acc = db.query(DriveAccount).filter_by(id=account_id).first()
    if not acc:
        raise HTTPException(404, detail="drive account not found")
    if acc.user_id != current_user.id:
        raise HTTPException(403, detail="access denied")
    vids = (
        db.query(DriveVideo)
        .filter(DriveVideo.account_id == account_id)
        .order_by(DriveVideo.posted_at.desc().nullslast(), DriveVideo.created_at.desc())
        .all()
    )
    return [v.to_dict() for v in vids]


@app.get("/api/drive/accounts/{account_id}/folders")
async def list_drive_folders(
    account_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """List folders the connected Drive can see — for the picker UI."""
    from models import DriveAccount
    from encryption import decrypt as _decrypt
    from drive_client import list_top_level_folders, DriveError
    acc = db.query(DriveAccount).filter_by(id=account_id).first()
    if not acc:
        raise HTTPException(404, detail="drive account not found")
    if acc.user_id != current_user.id:
        raise HTTPException(403, detail="access denied")
    try:
        refresh_token = _decrypt(acc.refresh_token_encrypted)
        folders = list_top_level_folders(refresh_token)
    except DriveError as e:
        raise HTTPException(500, detail=f"drive list error: {e}")
    return folders


# ============================================================================
# Local folder watcher (2026-06-02)
# Browser uses File System Access API (showDirectoryPicker) → JS polls the
# picked folder → uploads new .mp4/.mov/.webm files via multipart POST →
# backend ffmpeg + faster-whisper + match → advance to published with
# published_via='local_watch'. Per-user, no operator approval needed.
# ============================================================================

@app.get("/api/local-videos")
async def list_local_videos(
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    from models import LocalVideo
    vids = (
        db.query(LocalVideo)
        .filter(LocalVideo.user_id == current_user.id)
        .order_by(LocalVideo.created_at.desc())
        .limit(200)
        .all()
    )
    return [v.to_dict() for v in vids]


@app.post("/api/local-videos/upload")
async def upload_local_video(
    file: UploadFile = File(...),
    file_hash: str = Form(...),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Multipart endpoint for the browser watcher.

    file:      the video bytes (frontend reads with FileReader → Blob).
    file_hash: SHA-256 hex computed in the browser before upload — used for
               idempotent dedup. Re-uploading the same file is a no-op that
               returns the existing row.
    """
    from models import LocalVideo
    from local_transcribe import transcribe_local

    file_name = (file.filename or "(unnamed)")[:500]
    file_hash = (file_hash or "").strip().lower()
    if len(file_hash) != 64 or not all(c in "0123456789abcdef" for c in file_hash):
        raise HTTPException(400, detail="file_hash must be a 64-char SHA-256 hex string")

    # Idempotency: re-uploads of the same hash for the same user reuse the
    # existing row + skip re-processing.
    existing = (
        db.query(LocalVideo)
        .filter_by(user_id=current_user.id, file_hash=file_hash)
        .first()
    )
    if existing:
        return existing.to_dict()

    blob = await file.read()
    if not blob or len(blob) < 1024:
        raise HTTPException(400, detail=f"file too small ({len(blob)}B)")
    if len(blob) > 500 * 1024 * 1024:
        raise HTTPException(413, detail="file > 500MB")

    v = LocalVideo(
        user_id=current_user.id,
        file_hash=file_hash,
        file_name=file_name,
        size_bytes=len(blob),
        transcription_status="pending",
    )
    db.add(v)
    db.commit()
    db.refresh(v)

    # Synchronous transcribe — short enough for the request lifetime (Render
    # has a 60s+ HTTP timeout; ffmpeg + whisper on a 30s reel is ~10-20s).
    transcribe_local(v, blob, db)
    db.refresh(v)
    return v.to_dict()


@app.delete("/api/local-videos/by-hash/{file_hash}")
async def delete_local_video_by_hash(
    file_hash: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Called by the browser watcher when a file vanishes from the watched
    folder. If the LocalVideo had matched a Job and that Job's `published`
    state was set BY this local match (published_via='local_watch') AND
    no IG video has been linked yet (instagram_video_id IS NULL), revert
    the Job back to `awaiting_finishing` so it re-enters the match pool.

    Safety rails — do NOT revert when:
      - IG already linked the reel (instagram_video_id IS NOT NULL).
        The published state is genuinely correct, locally-deleted final-cut
        doesn't undo that.
      - published_via != 'local_watch' (something else published it).
    """
    from models import LocalVideo, Job
    file_hash = (file_hash or "").strip().lower()
    if len(file_hash) != 64 or not all(c in "0123456789abcdef" for c in file_hash):
        raise HTTPException(400, detail="file_hash must be a 64-char SHA-256 hex string")
    v = (
        db.query(LocalVideo)
        .filter_by(user_id=current_user.id, file_hash=file_hash)
        .first()
    )
    if not v:
        raise HTTPException(404, detail="local video not found")

    reverted_job_id = None
    if v.matched_job_id:
        job = db.query(Job).filter_by(id=v.matched_job_id).first()
        if (
            job
            and job.instagram_video_id is None
            and (job.published_via or "") == "local_watch"
        ):
            job.lifecycle_stage = "awaiting_finishing"
            job.published_via = None
            job.published_at = None
            reverted_job_id = job.id
    db.delete(v)
    db.commit()
    return {"deleted_hash": file_hash, "reverted_job_id": reverted_job_id}


@app.post("/api/drive/accounts/{account_id}/sync")
async def sync_drive_account(
    account_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Manual sync trigger — lists new files in the watched folder + queues
    transcription rows. Worker tick picks them up one-per-cycle afterwards."""
    from models import DriveAccount
    from drive_transcribe import sync_drive_folder
    from drive_client import DriveError
    acc = db.query(DriveAccount).filter_by(id=account_id).first()
    if not acc:
        raise HTTPException(404, detail="drive account not found")
    if acc.user_id != current_user.id:
        raise HTTPException(403, detail="access denied")
    if not acc.folder_id:
        raise HTTPException(400, detail="set a folder first")
    try:
        n = sync_drive_folder(acc, db)
    except DriveError as e:
        raise HTTPException(502, detail=f"drive sync error: {e}")
    return {"queued": n}


@app.patch("/api/jobs/{job_id}/lifecycle", response_model=JobResponse)
async def patch_job_lifecycle(
    job_id: str,
    req: UpdateLifecycleRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Advance, move-back, or annotate a Job's post-render lifecycle stage.

    See docs/superpowers/specs/2026-05-29-video-lifecycle-tracker-design.md §6.1.
    """
    job = get_user_job(db, job_id, current_user)
    try:
        apply_lifecycle_change(
            job,
            stage=req.stage,
            notes=req.notes,
            now=datetime.utcnow(),
            clear=req.clear,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    job.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(job)
    return _build_job_response(job, approved_clips=_count_approved_clips(db, job_id))


@app.patch("/api/jobs/{job_id}/archive", response_model=JobResponse)
async def patch_job_archive(
    job_id: str,
    req: UpdateArchiveRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Toggle the archived flag on a Job. Orthogonal to lifecycle stage."""
    job = get_user_job(db, job_id, current_user)
    job.archived = bool(req.archived)
    job.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(job)
    return _build_job_response(job, approved_clips=_count_approved_clips(db, job_id))


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

# v740 — image-attributable failure codes. A clip whose render failed with
# one of these codes can be rescued by the user uploading a different
# start_frame image (anchor / b-roll / persona reference) — the failure is
# attributable to the input image's content, not to transient infra issues.
#
# Sources:
#   - CONTENT_POLICY_VIOLATION    Banana 2 image-policy reject (image_platform path)
#   - CELEBRITY_FILTER            Veo celebrity-filter literal (worker.py:3083)
#   - CELEBRITY_RAI_FILTER        Veo enum value (config.py ErrorCode)
#   - SAFETY_FILTER               Veo safety-filter enum value
#   - ALL_IMAGES_BLACKLISTED      worker — every attached image rejected
#
# Pre-v740 only CONTENT_POLICY_VIOLATION qualified, so Veo-side voice-clip
# rejections (audio_pair whose start_frame is the v698A voiceover anchor
# image) had no upload-replacement path. v740 broadens both
# `replace_clip_image` (main.py) accept gate and the v710 image-shared
# cascade lookup gate to this set. Frontend mirrors the same set in
# `IMAGE_ATTRIBUTABLE_CODES` (static/index.html).
IMAGE_ATTRIBUTABLE_ERROR_CODES = frozenset({
    "CONTENT_POLICY_VIOLATION",
    "CELEBRITY_FILTER",
    "CELEBRITY_RAI_FILTER",
    "SAFETY_FILTER",
    "ALL_IMAGES_BLACKLISTED",
})


def _rewrite_r2_url_to_proxy(version: dict, job_id: str) -> dict:
    """v693 — rewrite legacy presigned R2 URLs in versions[].url to the
    backend proxy form. Pre-v693 the upload paths stored
    `storage.get_presigned_url(...)` (URLs of the form
    `https://<account>.r2.cloudflarestorage.com/<bucket>/...?Signature=...`)
    in clip versions_json. Users on networks that block
    *.r2.cloudflarestorage.com saw ERR_CONNECTION_TIMED_OUT when the
    frontend used those URLs directly. v693 changes the upload paths to
    store backend-proxy URLs instead, but legacy DB rows still carry the
    R2 host. Rewrite on read so all callers see proxy URLs uniformly.
    """
    url = version.get("url")
    if not url or not isinstance(url, str):
        return version
    if "r2.cloudflarestorage.com" in url or "amazonaws.com" in url:
        filename = version.get("filename")
        if not filename:
            attempt = version.get("attempt", 1)
            variant = version.get("variant", 1)
            return version  # cannot derive proxy URL without a filename
        version["url"] = f"/api/jobs/{job_id}/outputs/{filename}"
    return version


def deduplicate_versions(versions_json: str, job_id: Optional[str] = None) -> list:
    """Deduplicate versions by version_key (attempt.variant), keeping all unique variants.

    v693: when `job_id` is supplied, also rewrites any legacy presigned
    R2 URLs in the `url` field to the backend proxy form. Callers that
    don't have job_id handy can pass None and the rewrite is skipped
    (legacy URLs survive — they only break for ISP-blocked users).
    """
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

        # v693 — rewrite legacy R2 URLs to proxy form on read
        if job_id:
            v = _rewrite_r2_url_to_proxy(v, job_id)

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


def _restore_clip_to_prior_version(clip) -> Optional[Dict[str, Any]]:
    """v739 — restore a clip to its last good prior render from versions_json.

    Walks versions_json in reverse and picks the most recent entry that has
    a `filename`. Mutates clip in-place (status, output_*, attempt counters,
    error_* cleared, claim cleared). Caller commits.

    Returns {"filename", "attempt", "version_index"} on success, None if no
    prior version exists.

    Shared by:
      - POST /api/clips/{id}/cancel-redo (status-gated: redo_queued / flow_redo_queued / generating)
      - POST /api/clips/{id}/revert-to-prior-version (no status gate — works on FAILED stuck clips)

    The split exists because cancel-redo is the "abort a queued redo" semantic;
    revert-to-prior-version is the "rescue any stuck clip that has prior good
    output in versions_json" semantic. Same restore logic, different gate.
    """
    versions = json.loads(clip.versions_json) if clip.versions_json else []
    last_version = None
    last_index = None
    for idx in range(len(versions) - 1, -1, -1):
        v = versions[idx]
        if v.get("filename"):
            last_version = v
            last_index = idx
            break
    if not last_version:
        return None

    clip.status = ClipStatus.COMPLETED.value
    clip.approval_status = "pending_review"
    clip.output_filename = last_version["filename"]
    clip.output_url = last_version.get("url")
    restored_attempt = last_version.get(
        "attempt",
        max(1, (clip.generation_attempt or 1) - 1),
    )
    clip.generation_attempt = restored_attempt
    clip.selected_variant = last_index + 1  # 1-indexed pointer into versions[]
    clip.error_code = None
    clip.error_message = None
    clip.claimed_by_worker = None
    clip.claimed_at = None
    return {
        "filename": last_version["filename"],
        "attempt": restored_attempt,
        "version_index": last_index,
    }


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
            versions=deduplicate_versions(c.versions_json, job_id=c.job_id),
            selected_variant=c.selected_variant if c.selected_variant else 1,
            total_variants=get_actual_versions_count(c),
            clip_mode=c.clip_mode or "fresh",
            scene_index=c.scene_index or 0,
            prompt_text=c.prompt_text or None,
            in_lineup=c.id in lineup_set if lineup_set else True,
            # v698A
            clip_role=c.clip_role,
            paired_clip_id=c.paired_clip_id,
            voiceover_anchor_image_node_id=c.voiceover_anchor_image_node_id,
            voiceover_line=c.voiceover_line,
            # v718i (NEW 2026-05-18) — Veo native end-frame interpolation binding
            end_frame_image_node_id=c.end_frame_image_node_id,
            replacement_start_frame=c.replacement_start_frame,  # v701
        )
        for c in clips
    ]


@app.get("/api/jobs/{job_id}/clips/active", response_model=List[ClipResponse])
async def get_job_clips_active(
    job_id: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v727 — diff endpoint for the 5s clips poll.

    Returns ONLY clips whose status or approval state can still change:
      - status IN (pending, generating, retrying, redo_queued,
        flow_redo_queued, waiting_approval)
      - OR (status == completed AND approval_status == pending_review)

    The frontend's 5s ``selectJob`` poll calls this instead of the full
    ``/clips`` endpoint. Full endpoint reserved for initial selection,
    manual refresh, and after-mutation reload. Reduces a 50-clip job's
    per-poll payload from ~50 clips to typically 1-5.

    Response shape is identical to ``/clips`` so the client can merge
    rows into the local ``cachedClipsData`` map by ``id``.
    """
    from sqlalchemy import or_, and_

    job = get_user_job(db, job_id, current_user)

    ACTIVE_CLIP_STATUSES = (
        ClipStatus.PENDING.value,
        ClipStatus.GENERATING.value,
        ClipStatus.RETRYING.value,
        ClipStatus.REDO_QUEUED.value,
        ClipStatus.FLOW_REDO_QUEUED.value,
        ClipStatus.WAITING_APPROVAL.value,
    )
    clips = db.query(Clip).filter(
        Clip.job_id == job_id,
        or_(
            Clip.status.in_(ACTIVE_CLIP_STATUSES),
            and_(
                Clip.status == ClipStatus.COMPLETED.value,
                or_(
                    Clip.approval_status == "pending_review",
                    Clip.approval_status.is_(None),
                ),
            ),
        ),
    ).order_by(Clip.clip_index).all()

    lineup_set = None
    if job.clip_order_json:
        try:
            lineup_ids = json.loads(job.clip_order_json)
            lineup_set = set(lineup_ids)
        except (json.JSONDecodeError, KeyError):
            lineup_set = None

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
            versions=deduplicate_versions(c.versions_json, job_id=c.job_id),
            selected_variant=c.selected_variant if c.selected_variant else 1,
            total_variants=get_actual_versions_count(c),
            clip_mode=c.clip_mode or "fresh",
            scene_index=c.scene_index or 0,
            prompt_text=c.prompt_text or None,
            in_lineup=c.id in lineup_set if lineup_set else True,
            clip_role=c.clip_role,
            paired_clip_id=c.paired_clip_id,
            voiceover_anchor_image_node_id=c.voiceover_anchor_image_node_id,
            voiceover_line=c.voiceover_line,
            # v718i (NEW 2026-05-18) — Veo native end-frame interpolation binding
            end_frame_image_node_id=c.end_frame_image_node_id,
            replacement_start_frame=c.replacement_start_frame,
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


# ============ v701 — Image Policy Violation Replacement ============
# NOTE: the worker-auth /policy-violation endpoint is defined further
# below (after verify_local_worker_key is defined ~line 8553+) to avoid
# a NameError at module load. This block holds the user-auth half plus
# the request schema.

class PolicyViolationRequest(BaseModel):
    """Worker → backend report when Flow rejects start_frame for content
    policy. Carries the rejected frame's R2 key so the frontend can
    show it back to the user inside the replace-image card."""
    rejected_image_key: Optional[str] = None
    detail: Optional[str] = None  # Worker-side description if any


@app.post("/api/clips/{clip_id}/replace-image")
async def replace_clip_image(
    clip_id: int,
    file: UploadFile = File(...),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v701 — User uploads a replacement start_frame after Flow rejected
    the original on content policy. Uploads to R2 under the job's frames
    prefix, swaps Clip.start_frame to the new key, clears the policy-
    violation error, and resets clip status to PENDING so the worker
    redo path picks it up on the next poll.

    For audio_pair clips with a shared anchor: this endpoint replaces
    the start_frame on THE CALLED clip only. Cascading to all sibling
    audio_pairs that share the anchor is a Phase-2 enhancement; for now
    user re-uploads per affected clip (Phase 3a logic still binds them
    via paired_clip_id, so a future cascade can iterate that link).
    """
    from backends.storage import is_storage_configured, get_storage

    clip = get_user_clip(db, clip_id, current_user)

    # v740 — broadened from literal CONTENT_POLICY_VIOLATION to the set of
    # image-attributable failure codes (Banana 2 + Veo celebrity / safety /
    # all-images-blacklisted). Closes the audio_pair voice-clip stuck case
    # where Veo's anchor-image-side rejection set a non-CONTENT_POLICY code
    # and the user had no path to upload a different anchor face.
    if clip.error_code not in IMAGE_ATTRIBUTABLE_ERROR_CODES:
        raise HTTPException(
            status_code=400,
            detail="This clip is not awaiting an image replacement.",
        )
    print(
        f"[v740] image-attributable failure clip {clip.id} (code={clip.error_code!r}) "
        f"— upload-replacement path eligible",
        flush=True,
    )

    if not is_storage_configured():
        raise HTTPException(status_code=503, detail="Storage not configured")

    try:
        storage = get_storage()

        # Validate + read upload
        filename = (file.filename or "").lower()
        if not filename.endswith((".png", ".jpg", ".jpeg", ".webp")):
            raise HTTPException(
                status_code=400,
                detail="Image must be png / jpg / jpeg / webp.",
            )
        contents = await file.read()
        if len(contents) > 25 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="Image too large (>25MB).")
        if len(contents) < 1024:
            raise HTTPException(status_code=400, detail="Image too small / empty.")

        # Stash to local temp for storage.upload_job_frame.
        import tempfile
        ext = ".png"
        for _e in (".png", ".jpg", ".jpeg", ".webp"):
            if filename.endswith(_e):
                ext = _e
                break
        tmpfd, tmppath = tempfile.mkstemp(suffix=ext)
        try:
            os.write(tmpfd, contents)
        finally:
            os.close(tmpfd)

        # Build a new R2 key under the job's frames prefix. Timestamped to
        # keep audit history if the user uploads multiple replacements.
        from datetime import datetime as _dt
        ts = _dt.utcnow().strftime("%Y%m%dT%H%M%S")
        new_key_basename = f"replacement_clip{clip.id}_{ts}{ext}"
        await asyncio.to_thread(
            storage.upload_job_frame, clip.job_id, new_key_basename, tmppath
        )
        new_key = f"jobs/{clip.job_id}/frames/{new_key_basename}"

        # Audit: keep PREVIOUS rejected key in error_message tail; bump
        # start_frame to the fresh key so the worker's redo flow uses it.
        previous_rejected = clip.replacement_start_frame
        clip.start_frame = new_key
        clip.replacement_start_frame = previous_rejected  # keep audit
        clip.error_code = None
        clip.error_message = None
        # v701h — status MUST be flow_redo_queued so the worker's
        # /local-worker/clips/redo-pending poll picks the clip up. PENDING
        # status only gets re-submitted by the main /jobs/pending poll
        # which doesn't run for jobs already past initial submission. The
        # symptom: user uploaded replacement, clips sat in PENDING forever
        # because redo-pending query filters on FLOW_REDO_QUEUED.
        clip.status = ClipStatus.FLOW_REDO_QUEUED.value
        clip.approval_status = "pending_review"
        # Reset claim so worker picks it up.
        clip.claimed_by_worker = None
        clip.claimed_at = None
        db.commit()

        # v701d — anchor cascade.
        # If this clip is an audio_pair (its start_frame is the v698A
        # voiceover anchor image, which is SHARED across every audio_pair
        # in the job), the user's upload should propagate to ALL sibling
        # audio_pairs using the same anchor. Otherwise the user would have
        # to upload the same replacement N times — once per voiceover scene.
        cascade_count = 0
        patched_sibling_ids: set[int] = set()  # v710 — dedup across cascades
        try:
            clip_role = (clip.clip_role or '').lower()
            if clip_role == 'audio_pair' and clip.paired_clip_id:
                # Resolve the anchor binding via this clip's visual_pair sibling.
                # Phase 3a stores voiceover_anchor_image_node_id on the
                # visual_pair, NOT the audio_pair. So we hop:
                #   audio_pair → paired_clip_id → visual_pair → anchor_id
                visual_sibling = db.query(Clip).filter(
                    Clip.id == clip.paired_clip_id
                ).first()
                anchor_node_id = (
                    visual_sibling.voiceover_anchor_image_node_id
                    if visual_sibling else None
                )
                if anchor_node_id is not None:
                    # Find every OTHER audio_pair in this job whose visual_pair
                    # sibling references the same anchor. Skip the clip we
                    # just patched.
                    visual_with_same_anchor = db.query(Clip).filter(
                        Clip.job_id == clip.job_id,
                        Clip.clip_role == 'visual_pair',
                        Clip.voiceover_anchor_image_node_id == anchor_node_id,
                    ).all()
                    sibling_audio_ids = [
                        v.paired_clip_id for v in visual_with_same_anchor
                        if v.paired_clip_id and v.paired_clip_id != clip.id
                    ]
                    if sibling_audio_ids:
                        # v741 — filter cascade to ONLY siblings in image-
                        # attributable failure state. Pre-v741 the cascade
                        # patched every audio_pair sibling sharing the anchor
                        # regardless of status: COMPLETED voice clips that had
                        # already rendered fine got clobbered back to
                        # FLOW_REDO_QUEUED and re-rendered from scratch (lost
                        # the prior good render + burned fresh Veo credits).
                        # Mirror of v710 image-shared cascade gate which has
                        # always filtered on error_code — same discipline
                        # ports to v701d audio_pair anchor cascade now.
                        # Preserves COMPLETED (error_code NULL) + GENERATING
                        # (error_code NULL) + non-image-attributable failures
                        # (RATE_LIMIT / TIMEOUT / NETWORK — uploading new
                        # image doesn't fix those, separate retry path).
                        siblings = db.query(Clip).filter(
                            Clip.id.in_(sibling_audio_ids),
                            Clip.error_code.in_(list(IMAGE_ATTRIBUTABLE_ERROR_CODES)),
                        ).all()
                        skipped_count = len(sibling_audio_ids) - len(siblings)
                        if skipped_count > 0:
                            print(
                                f"[v741] anchor cascade preserved {skipped_count} "
                                f"audio_pair sibling(s) not in image-attributable "
                                f"failure state (likely COMPLETED — kept prior render)",
                                flush=True,
                            )
                        for sib in siblings:
                            sib.start_frame = new_key
                            sib.replacement_start_frame = (
                                sib.replacement_start_frame or sib.start_frame
                            )
                            sib.error_code = None
                            sib.error_message = None
                            # v701h — FLOW_REDO_QUEUED so redo-pending poll picks up.
                            sib.status = ClipStatus.FLOW_REDO_QUEUED.value
                            sib.approval_status = "pending_review"
                            sib.claimed_by_worker = None
                            sib.claimed_at = None
                            cascade_count += 1
                            patched_sibling_ids.add(sib.id)  # v710
                        db.commit()
        except Exception as _cascade_err:
            # v701-cleanup — full traceback so silent cascade failures
            # are visible. cavecrew flagged the bare except as a trap:
            # user uploads replacement, cascade fails, audio_pair siblings
            # silently NOT patched, user thinks job is fixed.
            import traceback
            print(
                f"[v701d] anchor cascade FAILED for clip {clip_id}: "
                f"{type(_cascade_err).__name__}: {_cascade_err}",
                flush=True,
            )
            traceback.print_exc()
            db.rollback()

        # v710 — image-shared replacement cascade.
        # Mirror of v701e's preemptive rejection cascade. When Flow rejects
        # clip K's start_frame on policy, v701e marks every OTHER clip in
        # the job sharing the same `start_frame` as CONTENT_POLICY_VIOLATION
        # so the worker stops burning credits on a doomed image. The
        # rejection-side comment at line 8769-8770 promised v701d would
        # patch those siblings back to pending on replacement — but v701d
        # only handles the audio_pair anchor relationship (different sibling
        # link). Result pre-v710: user uploads on clip 6, clip 7 (same
        # start_frame, marked by v701e) stayed FAILED + CONTENT_POLICY_VIOLATION
        # with the old rejected key forever. Frontend kept rendering the
        # "Rejected by Flow Content Policy" card; redo-pending poll never
        # picked it up (filter is FLOW_REDO_QUEUED only).
        #
        # Use `previous_rejected` (snapshotted at line 3602 BEFORE the
        # start_frame overwrite at line 3603) as the lookup key. Skip any
        # sibling already patched by v701d to avoid double-write.
        image_shared_count = 0
        try:
            rejected_lookup_key = previous_rejected
            if rejected_lookup_key:
                # v740 — broadened from literal CONTENT_POLICY_VIOLATION to
                # the IMAGE_ATTRIBUTABLE_ERROR_CODES set so v710 cascade
                # covers Veo celebrity / safety / blacklist failures on
                # siblings sharing the same start_frame too (e.g. when a
                # persona anchor image triggers both Banana 2 + Veo
                # rejections on different sibling clips).
                image_siblings_q = db.query(Clip).filter(
                    Clip.job_id == clip.job_id,
                    Clip.start_frame == rejected_lookup_key,
                    Clip.id != clip.id,
                    Clip.error_code.in_(list(IMAGE_ATTRIBUTABLE_ERROR_CODES)),
                )
                for sib in image_siblings_q.all():
                    if sib.id in patched_sibling_ids:
                        # Already patched via v701d audio_pair anchor cascade.
                        continue
                    sib.start_frame = new_key
                    # Preserve audit: don't clobber the original rejected key
                    # if it's already stashed.
                    sib.replacement_start_frame = (
                        sib.replacement_start_frame or rejected_lookup_key
                    )
                    sib.error_code = None
                    sib.error_message = None
                    sib.status = ClipStatus.FLOW_REDO_QUEUED.value
                    sib.approval_status = "pending_review"
                    sib.claimed_by_worker = None
                    sib.claimed_at = None
                    patched_sibling_ids.add(sib.id)
                    image_shared_count += 1
                if image_shared_count:
                    db.commit()
        except Exception as _img_cascade_err:
            import traceback
            print(
                f"[v710] image-shared cascade FAILED for clip {clip_id}: "
                f"{type(_img_cascade_err).__name__}: {_img_cascade_err}",
                flush=True,
            )
            traceback.print_exc()
            db.rollback()

        cascade_parts = []
        if cascade_count:
            cascade_parts.append(
                f"{cascade_count} audio twin"
                f"{'s' if cascade_count != 1 else ''}"
            )
        if image_shared_count:
            cascade_parts.append(
                f"{image_shared_count} image sibling"
                f"{'s' if image_shared_count != 1 else ''}"
            )
        cascade_msg = (
            f" (cascaded to {' + '.join(cascade_parts)})"
            if cascade_parts else ""
        )
        add_job_log(
            db, clip.job_id,
            f"Clip {clip.clip_index + 1}: user uploaded replacement image → re-queued{cascade_msg}",
            "INFO",
            "policy",
        )
        db.commit()

        # Cleanup tmp
        try:
            os.unlink(tmppath)
        except Exception:
            pass

        return {
            "ok": True,
            "clip_id": clip_id,
            "new_start_frame": new_key,
            "previous_rejected_frame": previous_rejected,
            "cascaded_audio_pair_count": cascade_count,  # v701d
            "cascaded_image_shared_count": image_shared_count,  # v710
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Replace failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# v735 — PATCH /api/clips/{clip_id}
#
# In-place edit of Clip row's scene-config + dialogue fields on an existing job
# without going back through re-import + Banana 2 re-render. Mirrors v725's
# PATCH /api/batches/{batch_id}/scenes/{scene_index} but on the post-promotion
# Clip table (jobs side) instead of pre-promotion ImageSceneAssignment.
#
# Use cases:
#   - v682b retrofit — drop dialogue_text (set to empty or NULL) on silent
#     b-roll scenes that incorrectly carried bracket markers. Veo TTS no
#     longer tries to literally speak "[upbeat music plays]".
#   - Dialogue rewrite — fix typos / safe-vocab swap without re-import.
#   - Mode flip — patch clip_mode (blend/fresh/continue) or cut_mode after
#     observing render drift on the first attempt.
#   - voiceover_anchor retrofit — fix v698A pair binding on a clip whose
#     anchor image was mis-bound at import.
#   - target_duration_s tweak — match observed Whisper-VAD trim length.
#   - prompt_text override — surgical Veo-prompt rewrite without touching
#     the source markdown / re-import.
#
# Allowed fields (each Optional[X]=None means "don't change"):
#   dialogue_text, dialogue_pad, prompt_text, clip_mode, cut_mode,
#   target_duration_s, veo_render_duration_s, caption, scene_type,
#   bg_color, clip_role, voiceover_anchor_image_node_id, voiceover_line
#   clear_fields: List[str] for explicit clear-to-NULL.
#
# Banned (would require dedicated flows / break invariants):
#   id, job_id, clip_index, status, start_frame, end_frame, output_filename,
#   output_url, error_code, error_message, approval_status, retry_count,
#   claimed_*, versions_json, generation_attempt, flow_clip_id, paired_clip_id
#
# Status guard: rejects PATCH when status == GENERATING (race with worker
# claim / submit). Operator pauses the job first, patches, then resumes.
# ─────────────────────────────────────────────────────────────────────────────

import re as _re_v735


class UpdateClipRequest(BaseModel):
    dialogue_text: Optional[str] = None
    dialogue_pad: Optional[str] = None
    prompt_text: Optional[str] = None
    clip_mode: Optional[str] = None
    cut_mode: Optional[str] = None
    target_duration_s: Optional[float] = None
    veo_render_duration_s: Optional[int] = None
    caption: Optional[str] = None
    scene_type: Optional[str] = None
    bg_color: Optional[str] = None
    clip_role: Optional[str] = None
    voiceover_anchor_image_node_id: Optional[int] = None
    voiceover_line: Optional[str] = None
    clear_fields: Optional[List[str]] = None


_V735_ALLOWED_CLEAR_FIELDS = {
    "dialogue_text", "dialogue_pad", "prompt_text", "cut_mode",
    "target_duration_s", "veo_render_duration_s", "caption",
    "scene_type", "bg_color", "voiceover_anchor_image_node_id",
    "voiceover_line",
}

_V735_HEX_COLOR_RE = _re_v735.compile(r"^#?(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$")


@app.patch("/api/clips/{clip_id}")
async def update_clip(
    clip_id: int,
    req: UpdateClipRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v735 — in-place PATCH of Clip scene-config + dialogue fields.

    See v735 header comment above for design rationale + use cases.
    """
    clip = get_user_clip(db, clip_id, current_user)

    # ─── Status guard ────────────────────────────────────────────────────
    if clip.status == ClipStatus.GENERATING.value:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Clip {clip_id} is currently GENERATING. Pause the job "
                f"first (POST /api/jobs/{clip.job_id}/pause), patch, then "
                f"resume. Patching mid-render races with the worker claim."
            ),
        )

    # ─── clip_mode ───────────────────────────────────────────────────────
    if req.clip_mode is not None:
        cm = req.clip_mode.lower().strip()
        if cm not in ("blend", "fresh", "continue"):
            raise HTTPException(
                400,
                f"Unrecognized clip_mode {req.clip_mode!r}; expected one of "
                f"'blend', 'fresh', 'continue'.",
            )
        clip.clip_mode = cm

    # ─── cut_mode ────────────────────────────────────────────────────────
    if req.cut_mode is not None:
        cmm = req.cut_mode.lower().strip()
        if cmm not in ("whisper", "timeline", "auto"):
            raise HTTPException(
                400,
                f"Unrecognized cut_mode {req.cut_mode!r}; expected one of "
                f"'whisper', 'timeline', 'auto'.",
            )
        clip.cut_mode = cmm

    # ─── scene_type ──────────────────────────────────────────────────────
    if req.scene_type is not None:
        st = req.scene_type.lower().strip()
        if st not in ("shot", "text_card"):
            raise HTTPException(
                400,
                f"Unrecognized scene_type {req.scene_type!r}; expected one "
                f"of 'shot', 'text_card'.",
            )
        clip.scene_type = st

    # ─── clip_role (v698A pair discriminator) ────────────────────────────
    if req.clip_role is not None:
        cr = req.clip_role.lower().strip()
        if cr not in ("single", "visual_pair", "audio_pair"):
            raise HTTPException(
                400,
                f"Unrecognized clip_role {req.clip_role!r}; expected one of "
                f"'single', 'visual_pair', 'audio_pair'.",
            )
        clip.clip_role = cr

    # ─── target_duration_s ───────────────────────────────────────────────
    if req.target_duration_s is not None:
        if not (0.1 <= req.target_duration_s <= 60.0):
            raise HTTPException(
                400,
                f"target_duration_s {req.target_duration_s} out of range "
                f"[0.1, 60.0]",
            )
        clip.target_duration_s = float(req.target_duration_s)

    # ─── veo_render_duration_s ───────────────────────────────────────────
    if req.veo_render_duration_s is not None:
        if int(req.veo_render_duration_s) not in (4, 6, 8):
            raise HTTPException(
                400,
                f"veo_render_duration_s {req.veo_render_duration_s} not in "
                f"Veo render buckets [4, 6, 8]",
            )
        clip.veo_render_duration_s = int(req.veo_render_duration_s)

    # ─── bg_color (hex) ──────────────────────────────────────────────────
    if req.bg_color is not None:
        bg = req.bg_color.strip()
        if bg and not _V735_HEX_COLOR_RE.match(bg):
            raise HTTPException(
                400,
                f"bg_color {req.bg_color!r} not a valid hex color "
                f"(expected #RGB, #RRGGBB, or #RRGGBBAA).",
            )
        clip.bg_color = bg if bg.startswith("#") or not bg else f"#{bg}"

    # ─── voiceover_anchor_image_node_id (v698A) ──────────────────────────
    if req.voiceover_anchor_image_node_id is not None:
        from models import ImageNode as _ImageNodeForAnchor  # local import
        anchor = db.query(_ImageNodeForAnchor).filter(
            _ImageNodeForAnchor.id == req.voiceover_anchor_image_node_id,
            _ImageNodeForAnchor.user_id == current_user.id,
        ).first()
        if not anchor:
            raise HTTPException(
                400,
                f"voiceover_anchor_image_node_id "
                f"{req.voiceover_anchor_image_node_id} not found for current "
                f"user.",
            )
        if (anchor.role or "").lower() != "voiceover_anchor":
            raise HTTPException(
                400,
                f"Node {anchor.id} has role={anchor.role!r}, expected "
                f"'voiceover_anchor' (per v698A image-role discriminator).",
            )
        clip.voiceover_anchor_image_node_id = anchor.id

    # ─── Text fields (no enum / range validation) ────────────────────────
    if req.dialogue_text is not None:
        clip.dialogue_text = req.dialogue_text
    if req.dialogue_pad is not None:
        clip.dialogue_pad = req.dialogue_pad
    if req.prompt_text is not None:
        clip.prompt_text = req.prompt_text
    if req.caption is not None:
        clip.caption = req.caption
    if req.voiceover_line is not None:
        clip.voiceover_line = req.voiceover_line

    # ─── Auto-clear anchor + voiceover_line when leaving visual_pair ─────
    # Mirror v725 auto-clear: flipping clip_role away from visual_pair
    # leaves orphan anchor refs. Clear unless caller explicitly set them
    # in the same PATCH.
    if req.clip_role is not None and clip.clip_role != "visual_pair":
        if req.voiceover_anchor_image_node_id is None:
            if clip.voiceover_anchor_image_node_id is not None:
                print(
                    f"[v735] Auto-clearing voiceover_anchor_image_node_id on "
                    f"clip {clip_id} (clip_role={clip.clip_role!r} is not "
                    f"'visual_pair')",
                    flush=True,
                )
                clip.voiceover_anchor_image_node_id = None
        if req.voiceover_line is None and clip.voiceover_line is not None:
            print(
                f"[v735] Auto-clearing voiceover_line on clip {clip_id} "
                f"(clip_role={clip.clip_role!r} is not 'visual_pair')",
                flush=True,
            )
            clip.voiceover_line = None

    # ─── Validation: visual_pair requires anchor ─────────────────────────
    if clip.clip_role == "visual_pair" and clip.voiceover_anchor_image_node_id is None:
        raise HTTPException(
            400,
            "clip_role='visual_pair' requires voiceover_anchor_image_node_id "
            "to be set (per v698A). Set it in the same PATCH or change "
            "clip_role to 'single'.",
        )

    # ─── Validation: text_card requires caption + bg_color ───────────────
    if clip.scene_type == "text_card":
        if not (clip.caption or "").strip():
            raise HTTPException(
                400,
                "scene_type='text_card' requires non-empty caption.",
            )
        if not (clip.bg_color or "").strip():
            raise HTTPException(
                400,
                "scene_type='text_card' requires bg_color hex.",
            )

    # ─── clear_fields — explicit clear-to-NULL ───────────────────────────
    cleared = []
    if req.clear_fields:
        for f in req.clear_fields:
            if f not in _V735_ALLOWED_CLEAR_FIELDS:
                raise HTTPException(
                    400,
                    f"clear_fields field {f!r} not in allow-list "
                    f"{sorted(_V735_ALLOWED_CLEAR_FIELDS)}",
                )
            setattr(clip, f, None)
            cleared.append(f)

    try:
        db.commit()
        db.refresh(clip)
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"PATCH commit failed: {e}")

    # Audit log entry (best-effort)
    try:
        _changed = []
        for fname in (
            "dialogue_text", "dialogue_pad", "prompt_text", "clip_mode",
            "cut_mode", "target_duration_s", "veo_render_duration_s",
            "caption", "scene_type", "bg_color", "clip_role",
            "voiceover_anchor_image_node_id", "voiceover_line",
        ):
            if getattr(req, fname) is not None:
                _changed.append(fname)
        add_job_log(
            db, clip.job_id,
            f"[v735] Clip {clip.clip_index + 1} patched — "
            f"changed: {', '.join(_changed) or 'none'}"
            + (f" — cleared: {', '.join(cleared)}" if cleared else ""),
            source="user",
        )
    except Exception:
        pass

    return {
        "ok": True,
        "clip_id": clip.id,
        "clip_index": clip.clip_index,
        "changed_fields": [
            f for f in (
                "dialogue_text", "dialogue_pad", "prompt_text", "clip_mode",
                "cut_mode", "target_duration_s", "veo_render_duration_s",
                "caption", "scene_type", "bg_color", "clip_role",
                "voiceover_anchor_image_node_id", "voiceover_line",
            ) if getattr(req, f) is not None
        ],
        "cleared_fields": cleared,
        "clip": clip.to_dict(),
    }


@app.post("/api/clips/{clip_id}/upload-variant")
async def upload_clip_variant(
    clip_id: int,
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
):
    """v757 — Operator uploads their own finished video for a clip. The file
    is stored in R2 under the job's outputs prefix and appended to the clip's
    versions_json as a NEW variant, then selected. Resolves rejected/failed
    clips (status -> completed, approval_status -> pending_review). No worker
    involvement.

    Follows the worker upload-video pattern (local_worker_upload_video): the R2
    upload is slow, so NO DB connection is held during it (v507) — brief
    sessions only, before and after the upload, the second under a
    with_for_update() row lock so a concurrent worker variant upload cannot
    clobber versions_json.
    """
    from backends.storage import is_storage_configured, get_storage
    from models import get_db
    from datetime import datetime as _dt
    from uuid import uuid4

    if not is_storage_configured():
        raise HTTPException(status_code=503, detail="Storage not configured")

    # Phase 1 — brief session: authorize + read identifiers, then release.
    # get_user_clip raises if the clip is not owned by current_user, so this
    # gates the WHOLE request: an unauthorized caller never reaches the upload
    # or the Phase-3 mutation below (clip_id is immutable across phases).
    with get_db() as db:
        clip = get_user_clip(db, clip_id, current_user)
        job_id = clip.job_id
        clip_index = clip.clip_index
        attempt = clip.generation_attempt or 1

    # Validate + read upload (accept any video; no aspect/duration checks per spec).
    contents = await file.read()
    if len(contents) < 1024:
        raise HTTPException(status_code=400, detail="Video too small / empty.")
    if len(contents) > 200 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Video too large (>200MB).")

    import tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
        tmp.write(contents)
        tmp_path = tmp.name

    try:
        storage = get_storage()

        # Unique, collision-free filename (variant number is assigned later under
        # the lock; it does NOT need to be encoded in the filename — export and
        # the proxy URL use output_filename verbatim as the R2 outputs key).
        ts = _dt.utcnow().strftime("%Y%m%dT%H%M%S")
        output_filename = f"clip_{clip_index}_user_{ts}_{uuid4().hex[:8]}.mp4"
        r2_key = f"jobs/{job_id}/outputs/{output_filename}"
        output_url = f"/api/jobs/{job_id}/outputs/{output_filename}"

        # Phase 2 — R2 upload, NO DB connection held.
        await asyncio.to_thread(storage.upload_file, tmp_path, r2_key, "video/mp4")

        # Phase 3 — brief locked session: append variant + point clip at it.
        with get_db() as db:
            clip = db.query(Clip).filter(Clip.id == clip_id).with_for_update().first()
            if not clip:
                raise HTTPException(status_code=404, detail="Clip not found")

            versions = json.loads(clip.versions_json) if clip.versions_json else []
            existing = [v.get("variant", 1) for v in versions if v.get("attempt", 1) == attempt]
            variant = (max(existing) if existing else 0) + 1

            version_entry = {
                "attempt": attempt,
                "variant": variant,
                "version_key": f"{attempt}.{variant}",
                "filename": output_filename,
                "url": output_url,
                "generated_at": _dt.utcnow().isoformat(),
                "source": "user_upload",
            }
            versions.append(version_entry)
            versions.sort(key=lambda x: (x.get("attempt", 1), x.get("variant", 1)))
            clip.versions_json = json.dumps(versions)

            # Point the clip at the freshly uploaded variant (mirror select-variant).
            clip.output_filename = output_filename
            clip.output_url = output_url
            clip.selected_variant = variant
            # Resolve the clip so it stops blocking; operator reviews their upload.
            clip.status = ClipStatus.COMPLETED.value
            clip.approval_status = "pending_review"
            clip.error_code = None
            clip.error_message = None

            add_job_log(
                db, job_id,
                f"Clip {clip_index + 1} variant {attempt}.{variant} uploaded by user",
                "INFO", "user",
            )
            db.commit()
            clip_dict = clip.to_dict()

        return {"success": True, "clip": clip_dict, "variant": variant, "url": output_url}
    except HTTPException:
        raise
    except Exception as e:
        print(f"[UploadVariant] error clip {clip_id}: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


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

    # v739 — delegate restore logic to shared helper.
    restored = _restore_clip_to_prior_version(clip)

    if not restored:
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

    db.commit()

    add_job_log(db, clip.job_id, f"Clip {clip.clip_index + 1} redo cancelled — reverted to {restored['filename']}", "INFO", "redo_cancel")

    return {"success": True, "message": f"Reverted to previous version", "filename": restored['filename']}


@app.post("/api/clips/{clip_id}/revert-to-prior-version")
async def revert_clip_to_prior_version(
    clip_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v739 — universal stuck-clip rescue. Restore a clip to its last good
    prior render from versions_json, regardless of current status.

    Unlike cancel-redo (which gates on redo_queued / flow_redo_queued /
    generating), this endpoint has NO status gate. It works when:
      - clip is FAILED with error_code = CONTENT_POLICY_VIOLATION
        (Flow rejected the redo's start_frame; the prior render is fine)
      - clip is FAILED with error_code = REDO_STUCK
      - clip is FAILED for any other reason but versions_json has a prior good entry
      - clip is COMPLETED with multiple variants and user wants to roll back

    Only requirement: at least one entry in versions_json carries a `filename`.

    Paired-clip cascade (atomic UI unit):
      visual_pair + audio_pair render as a single card in the frontend; if
      one was rejected and the user wants the prior render back, the paired
      sibling should revert too (when it has a prior version). Best-effort:
      if the paired sibling has no versions_json entry with filename, leave
      it alone and report cascaded_paired=False — the user gets the calling
      clip back and can deal with the orphan paired side separately.

    Mirror of v701d / v710 cascade discipline: log full traceback on cascade
    failure, never swallow silently.
    """
    clip = get_user_clip(db, clip_id, current_user)

    restored = _restore_clip_to_prior_version(clip)
    if not restored:
        raise HTTPException(
            status_code=400,
            detail="No prior version with a rendered output exists for this clip. Use redo or upload replacement instead.",
        )

    # Paired-clip cascade. Audio_pair + visual_pair siblings are an atomic
    # render unit; if one was redone (then got stuck) the other typically
    # was too. Revert paired sibling when it also has a prior good version.
    cascaded_paired = False
    paired_filename: Optional[str] = None
    try:
        if clip.paired_clip_id:
            paired = db.query(Clip).filter(Clip.id == clip.paired_clip_id).first()
            if paired is not None:
                paired_restored = _restore_clip_to_prior_version(paired)
                if paired_restored:
                    cascaded_paired = True
                    paired_filename = paired_restored["filename"]
                    print(
                        f"[v739] paired cascade ✓ clip {clip_id} paired_id={paired.id} "
                        f"restored to attempt {paired_restored['attempt']} "
                        f"(filename={paired_filename})",
                        flush=True,
                    )
                else:
                    print(
                        f"[v739] paired cascade ⊘ clip {clip_id} paired_id={paired.id} "
                        f"has no prior version with filename — leaving paired alone",
                        flush=True,
                    )
    except Exception as _paired_err:
        import traceback
        print(
            f"[v739] paired cascade FAILED for clip {clip_id}: "
            f"{type(_paired_err).__name__}: {_paired_err}",
            flush=True,
        )
        traceback.print_exc()
        # Don't rollback the primary restore — paired is best-effort.
        # The primary clip should still come back even if paired errored.

    db.commit()

    print(
        f"[v739] revert clip {clip_id} → attempt {restored['attempt']} "
        f"(filename={restored['filename']}, paired_cascaded={cascaded_paired})",
        flush=True,
    )

    cascade_msg = ""
    if cascaded_paired:
        cascade_msg = " (paired clip also reverted)"
    add_job_log(
        db, clip.job_id,
        f"Clip {clip.clip_index + 1} reverted to prior render: {restored['filename']}{cascade_msg}",
        "INFO",
        "revert",
    )

    return {
        "success": True,
        "message": f"Reverted to prior version: {restored['filename']}",
        "filename": restored["filename"],
        "attempt": restored["attempt"],
        "version_index": restored["version_index"],
        "cascaded_paired": cascaded_paired,
        "paired_filename": paired_filename,
    }


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

    # v761d DIAGNOSTIC — operator reports "Retry stuck (1)" shows in the UI
    # but the endpoint returns reset_count=0 ("No stuck clips found"). The
    # frontend stuck-count and this endpoint's reset criteria diverge. Log
    # every clip's status + claim/age + skip reason so the next operator
    # click reveals WHY a visibly-stuck clip is not being re-queued.
    # Remove once root cause confirmed.
    _diag = []
    for clip in candidates:
        _st = clip.status
        _ca = getattr(clip, 'claimed_at', None)
        _ua = getattr(clip, 'updated_at', None) or getattr(clip, 'created_at', None)
        _age_c = (now - _ca).total_seconds() if _ca else None
        _age_u = (now - _ua).total_seconds() if _ua else None
        _decision = "skip(status-not-covered)"
        if _st == ClipStatus.PENDING.value:
            _decision = "reset(pending)"
        elif _st == ClipStatus.GENERATING.value:
            _decision = "reset(stale-generating)" if (_ca is None or _ca < stale_cutoff) else "skip(generating-fresh-claim)"
        elif _st in (ClipStatus.REDO_QUEUED.value, ClipStatus.FLOW_REDO_QUEUED.value):
            _decision = "reset(stale-redo)" if (_ua is None or _ua < stale_cutoff) else "skip(redo-fresh)"
        _diag.append(f"clip#{getattr(clip,'clip_index','?')} status={_st} claimed_age={_age_c}s upd_age={_age_u}s -> {_decision}")
    print(f"[retry-stuck v761d] job={job_id} backend={job.backend} clips={len(candidates)}", flush=True)
    for _d in _diag:
        print(f"[retry-stuck v761d]   {_d}", flush=True)

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
        # v761d — surface the per-clip diagnostic into the job log too, so
        # the operator can read it without Render log access.
        add_job_log(
            db, job_id,
            "retry-stuck found 0 resettable clips. Per-clip: " + " | ".join(_diag),
            "INFO", "system",
        )
        db.commit()
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

    # Self-heal has_export: a job whose R2 outputs already contain a final
    # export (final_export_ / final_broll_ / export_) WAS exported, even if
    # the flag was never set — pre-v776 exports (the setter didn't exist
    # yet) and voice-swap-only b-roll paths (set has_voice_clone, not
    # has_export). Without this such jobs sit forever at awaiting_export
    # (derive_effective_stage gates the finishing transition on has_export)
    # and get excluded from the IG-match candidate pool. Setting the flag +
    # advancing the stage the moment the job is viewed repairs it with no
    # manual backfill. Guarded so it only fires when a final export exists.
    if job and not getattr(job, 'has_export', False):
        _has_final_export = any(
            (v.get("filename") or "").startswith(("final_export_", "final_broll_", "export_"))
            for v in videos
        )
        if _has_final_export:
            job.has_export = True
            # v783 (2026-06-05): final-export-exists implies the job is done.
            # Repair status alongside has_export so the badge stops showing
            # PROCESSING for jobs that an earlier Flow redo (L12816) left
            # status-reverted. Skip cancelled/failed (terminal operator intent).
            if job.status not in (JobStatus.CANCELLED.value, JobStatus.FAILED.value, JobStatus.COMPLETED.value):
                job.status = JobStatus.COMPLETED.value
                if job.completed_at is None:
                    job.completed_at = datetime.utcnow()
            _maybe_auto_enter_lifecycle(job, now=datetime.utcnow())
            db.commit()
            print(f"[Outputs] self-heal: job={job_id[:8]} has a final export "
                  f"→ has_export=True, status={job.status}, lifecycle_stage={job.lifecycle_stage}", flush=True)

    return {"job_id": job_id, "videos": videos, "count": len(videos)}


@app.get("/api/jobs/{job_id}/outputs/{filename}")
async def download_output(
    job_id: str,
    filename: str,
    request: Request,
    download: int = 0,
):
    """Download a generated video. Works with local filesystem or R2 storage.

    v76x: when `?download=1` is set, force a browser save dialog instead of
    inline playback. The R2 redirect path defaults to `Content-Disposition:
    inline`, so `<a download="...">` clicks on the cross-origin presigned URL
    open the .mp4 in a new tab. Setting an explicit `response-content-
    disposition=attachment; filename="..."` override on the presign makes R2
    serve the file with `Content-Disposition: attachment`, which every
    browser honors regardless of origin. The local FileResponse path is
    already attachment-by-default (Starlette FileResponse with `filename=...`
    sets `Content-Disposition: attachment` when `content_disposition_type`
    defaults to "attachment"), but we explicitly pass it for clarity.
    """
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
    # v754: was "private" — that FORBADE Cloudflare (in front of Render) from
    # edge-caching clips, so every clip load on every device hit the single
    # 1-CPU origin and proxied bytes through it (the "everything loads slow").
    # Operator OK'd public media (UUID filenames are unguessable). "public"
    # lets Cloudflare cache each clip at the edge after the first fetch, so
    # repeat/other-device loads skip the origin entirely and the origin CPU is
    # freed for fresh redos. Same bytes for a given URL (no per-user variance),
    # so shared caching is safe.
    video_cache_headers = {"Cache-Control": "public, max-age=31536000, immutable"}
    if filepath.exists():
        # v76x: when ?download=1, force attachment so the browser saves the
        # file instead of opening it inline. FileResponse with filename=...
        # already serves Content-Disposition: attachment by default; we pass
        # content_disposition_type explicitly so this stays correct even if
        # Starlette ever changes its default.
        cd_type = "attachment" if download else "inline"
        return FileResponse(
            filepath,
            media_type="video/mp4",
            filename=filename,
            headers=video_cache_headers,
            content_disposition_type=cd_type,
        )

    # Method 2: R2 storage — v75x: REDIRECT the player straight to a presigned
    # R2 URL instead of proxying the bytes through this 1-CPU origin.
    #
    # Why this replaces the v753 proxy-stream: every clip request authed
    # (session cookie) and returned 206 range partials. Cloudflare caches
    # NEITHER cookie-bearing requests NOR 206 partials, so despite the v754
    # public,immutable header EVERY clip byte funneled through the single CPU
    # + a per-request DB-auth query -> playback stalled and re-buffered (~1s
    # then stuck). Redirecting puts R2 (native Range/seeking, real bandwidth,
    # geo-distributed) in the byte path; the origin only does the cheap auth
    # + sign, so it can handle many concurrent players.
    #
    # v695 footgun guard: the 302 MUST be no-store. The redirect removed in
    # v695 inherited the year-long video cache header, so the browser cached
    # the 302; when the 1h presign expired the cached redirect pointed at a
    # dead URL -> 404 on older clips. no-store makes the browser always re-hit
    # this (cheap) endpoint for a FRESH presign. The bytes are cached by R2 /
    # the browser via the presigned response's own headers, not via the 302.
    try:
        from backends.storage import is_storage_configured, get_storage

        if is_storage_configured():
            storage = get_storage()
            # generate_presigned_url is a local signing op (no network), so no
            # to_thread needed. 24h expiry comfortably outlasts a review
            # session; no-store means an expired presign can never be replayed.
            # v76x: when ?download=1, sign the URL with a Content-Disposition
            # override so R2 returns the bytes as `attachment; filename="..."`
            # — the only way to make a cross-origin .mp4 download instead of
            # play in a new tab. Strip embedded quotes/CR/LF from the filename
            # before interpolating to keep the header well-formed.
            disposition = None
            if download:
                safe_name = filename.replace('"', '').replace('\r', '').replace('\n', '')
                disposition = f'attachment; filename="{safe_name}"'
            presigned = storage.get_job_output_url(
                job_id,
                filename,
                expires_in=86400,
                response_content_disposition=disposition,
            )
            from fastapi.responses import RedirectResponse
            print(
                f"[Download v76x] REDIRECT {filename} -> presigned R2 "
                f"(no-store 302, download={bool(download)})",
                flush=True,
            )
            return RedirectResponse(
                url=presigned,
                status_code=302,
                headers={"Cache-Control": "no-store"},
            )
    except HTTPException:
        raise
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
                    start_score = difflib.SequenceMatcher(None, clip_start, line_start, autojunk=False).ratio()
                    # autojunk=False: full lines can exceed 200 chars (b-roll
                    # voiceover), where difflib junks common chars and tanks
                    # the ratio. Same root cause as instagram_match score().
                    full_score = difflib.SequenceMatcher(None, normalize_text(t['text']), normalize_text(line), autojunk=False).ratio()
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
                # autojunk=False: dial / clip text can exceed 200 chars on
                # b-roll voiceover lines, where difflib junks common chars and
                # tanks the ratio. Same root cause as instagram_match score().
                score = difflib.SequenceMatcher(None, dial.lower(), t['text'].lower(), autojunk=False).ratio()
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
    
    # v689 — auto-approve text_card clips at export time. Pre-v688 the
    # Clip writer didn't auto-approve text_card scenes (they stayed at
    # 'pending_review'), so the export's approval filter excluded them
    # and the final video missed the text card entirely.
    _text_card_clips = db.query(Clip).filter(
        Clip.job_id == job_id,
        Clip.scene_type == "text_card",
        Clip.approval_status != "approved",
    ).all()
    if _text_card_clips:
        for tc in _text_card_clips:
            tc.approval_status = "approved"
            if tc.status != ClipStatus.COMPLETED.value:
                tc.status = ClipStatus.COMPLETED.value
        db.commit()
        print(
            f"[Export][v689] auto-approved {len(_text_card_clips)} text_card "
            f"clip(s) so the export includes them (drawtext at concat).",
            flush=True,
        )

    # v690 — RECREATE missing text_card clips from source_markdown /
    # storyboard. User scenario: text_card clip got DELETED via the
    # Review & Approve UI (because of an earlier blocking bug pre-v688
    # where the clip was stuck at 'preparing' / 'redo failed'). The
    # delete removed the Clip row entirely, so v689's auto-approve
    # has nothing to update. The text card is now MISSING from the
    # final mp4 even though the markdown's `## Storyboard` still has
    # `scene_type: text_card` for that scene_index.
    #
    # Backfill: parse the dialogue_json's scenes structure (or the
    # batch's storyboard via promoted_video_job_id). For each
    # storyboard scene with scene_type='text_card', check if a Clip
    # row exists at the clip_index that scene maps to. If missing,
    # INSERT a synthetic clip row with the markdown's caption /
    # bg_color / duration_s, mark COMPLETED + approved, and
    # renumber following clip_index values is NOT needed — clip_index
    # 0..N-1 stays the same as long as the missing slot's index
    # is recreated.
    try:
        _dlg = json.loads(job.dialogue_json) if job.dialogue_json else {}
        _scenes_md = _dlg.get("scenes", []) or []
        _lines_md = _dlg.get("lines", []) or []
        # Build clip_index → scene_type/caption/bg_color/duration map
        # from the dialogue payload's lines (DialogueLineInput entries).
        _missing_recreated = 0
        for _idx, _line in enumerate(_lines_md):
            if not isinstance(_line, dict):
                continue
            if (_line.get("scene_type") or "").lower() != "text_card":
                continue
            # Check if a Clip row exists at this clip_index
            _existing = db.query(Clip).filter(
                Clip.job_id == job_id,
                Clip.clip_index == _idx,
            ).first()
            if _existing is not None:
                continue
            # Recreate
            _new_clip = Clip(
                job_id=job_id,
                clip_index=_idx,
                dialogue_id=_idx + 1,
                dialogue_text=_line.get("text", "") or "",
                status=ClipStatus.COMPLETED.value,
                approval_status="approved",
                scene_index=_line.get("scene_index"),
                clip_mode=_line.get("clip_mode") or "fresh",  # v782 default fresh
                caption=_line.get("caption"),
                scene_type="text_card",
                bg_color=_line.get("bg_color") or "black",
                target_duration_s=float(_line.get("duration_s") or 1.0),
                completed_at=datetime.utcnow(),
                prompt_text=(
                    f"[text_card placeholder — caption: "
                    f"{(_line.get('caption') or '').strip()!r}, "
                    f"bg: {(_line.get('bg_color') or 'black').strip()}, "
                    f"duration: {_line.get('duration_s') or 1.0}s]"
                ),
            )
            db.add(_new_clip)
            _missing_recreated += 1
        if _missing_recreated:
            db.commit()
            print(
                f"[Export][v690] recreated {_missing_recreated} missing "
                f"text_card clip(s) from dialogue_json (drawtext at concat).",
                flush=True,
            )
    except Exception as _e:
        print(f"[Export][v690] text_card backfill skipped (non-fatal): {_e}", flush=True)

    # v692 — backfill missing cut_mode + target_duration_s on Clip rows by
    # re-parsing the source markdown's frame_anchor_s deltas. Required when
    # the job was prepared BEFORE v667/v668 wired anchor-derived durations
    # into prepare_batch_for_video. Without this, timeline-mode clips fall
    # through _trim_one's `cm == "timeline" and td and td > 0` guard at
    # video_processor.py:3377 and end up frame-trimmed to ~full source
    # duration (~30s each * 8 clips ≈ 233s final, vs the intended ~32s).
    # That oversized concat then OOMs the speed-apply ffmpeg pass.
    try:
        from image_platform import ImageJobBatch, parse_scene_table
        _batch = db.query(ImageJobBatch).filter(
            ImageJobBatch.promoted_video_job_id == job_id
        ).first()
        _md = _batch.source_markdown if _batch else None
        if _md:
            _parsed = parse_scene_table(_md)
            _images = {img["image_index"]: img for img in _parsed.get("images", [])}
            _scenes_md = _parsed.get("scenes", [])
            # scene_index → frame_anchor_s (from image lookup)
            _anchors_in_order = []
            _scene_md_meta = {}
            for s in sorted(_scenes_md, key=lambda x: x["scene_index"]):
                img_idx = s.get("image_index")
                anchor = None
                if img_idx is not None:
                    anchor = _images.get(img_idx, {}).get("frame_anchor_s")
                _anchors_in_order.append((s["scene_index"], anchor))
                _scene_md_meta[s["scene_index"]] = {
                    "cut_mode": s.get("cut_mode"),
                    "scene_type": s.get("scene_type"),
                }

            def _next_distinct_anchor(sidx: int):
                cur = None
                for s_idx, a in _anchors_in_order:
                    if s_idx == sidx and a is not None:
                        cur = a
                        break
                if cur is None:
                    return None
                for s_idx, a in _anchors_in_order:
                    if s_idx > sidx and a is not None and a > cur:
                        return a
                return None

            _scene_targets = {}
            for sidx, a in _anchors_in_order:
                if a is None:
                    continue
                nxt = _next_distinct_anchor(sidx)
                if nxt is not None and nxt > a:
                    _scene_targets[sidx] = round(nxt - a, 3)

            _filled_cm = 0
            _filled_td = 0
            _all_clips = db.query(Clip).filter(Clip.job_id == job_id).all()
            for c in _all_clips:
                meta = _scene_md_meta.get(c.scene_index or 0)
                if not meta:
                    continue
                if not c.cut_mode and meta.get("cut_mode"):
                    c.cut_mode = meta["cut_mode"]
                    _filled_cm += 1
                if (c.cut_mode or "").lower() == "timeline" and not c.target_duration_s:
                    td = _scene_targets.get(c.scene_index or 0)
                    if td and td > 0:
                        c.target_duration_s = float(td)
                        _filled_td += 1
            if _filled_cm or _filled_td:
                db.commit()
                print(
                    f"[Export][v692] backfilled cut_mode on {_filled_cm} clip(s), "
                    f"target_duration_s on {_filled_td} clip(s) from frame_anchor deltas",
                    flush=True,
                )
            else:
                print(
                    f"[Export][v692] no cut_mode/target_duration_s gaps to backfill "
                    f"(scenes_md={len(_scene_md_meta)}, targets_md={len(_scene_targets)})",
                    flush=True,
                )
    except Exception as _e:
        print(f"[Export][v692] anchor backfill skipped (non-fatal): {_e}", flush=True)

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
                # v691c — propagate dialogue_pad so per-clip Whisper-VAD
                # has the full audio context (line + pad) for matching.
                "dialogue_pad": clip.dialogue_pad or "",
                # v698A — clip-pair metadata. clip_role lets the export
                # dual-output branch filter visual_pair clips out of the
                # speaker pipeline and out of (or into) the broll pipeline.
                # paired_clip_id lets the broll audio swap step look up
                # the audio_pair sibling.
                "clip_role": clip.clip_role,
                "paired_clip_id": clip.paired_clip_id,
                "voiceover_anchor_image_node_id": clip.voiceover_anchor_image_node_id,
                "voiceover_line": clip.voiceover_line,
                "scene_index": clip.scene_index,
                "_clip_db_id": clip.id,
                "_order": pos
            }
        return None

    from concurrent.futures import ThreadPoolExecutor as _TPE
    print(f"[Export] Downloading {len(clips)} clips from R2 in parallel (3 workers)...")
    with _TPE(max_workers=3) as pool:
        results = list(pool.map(_download_clip, list(enumerate(clips))))

    # v701n — Sort with audio_pair interleaved next to its visual_pair.
    # Bug: audio_pair Clip rows are written with
    # clip_index = 100000 + vp.clip_index (main.py:2595), so a naive
    # clip_index-ASC sort pushes ALL audio_pair entries to the END of the
    # export. Speaker pipeline (which DROPS visual_pair) then concatenates:
    # HOOK → CTA → 9 voiceovers (wrong) instead of HOOK → 9 voiceovers → CTA.
    # Fix: when a row's clip_role == 'audio_pair', sort it at its paired
    # visual_pair's clip_index (secondary key 1 → lands right after the
    # visual_pair). Lineup-override case keeps _order untouched.
    _non_null = [r for r in results if r is not None]
    if job.clip_order_json:
        # Custom lineup — user authored the order explicitly; preserve _order
        clip_info = sorted(_non_null, key=lambda x: x["_order"])
    else:
        # Build paired_id → visual_pair clip_index lookup
        _vp_idx_by_id = {
            r.get("_clip_db_id"): r["clip_index"]
            for r in _non_null
            if (r.get("clip_role") or "").lower() == "visual_pair"
        }

        def _interleaved_key(r):
            role = (r.get("clip_role") or "single").lower()
            if role == "audio_pair":
                paired_id = r.get("paired_clip_id")
                vp_idx = _vp_idx_by_id.get(paired_id)
                if vp_idx is not None:
                    # Land right after the paired visual_pair
                    return (vp_idx, 1)
                # Orphan audio_pair — fall back to its own clip_index
                return (r["clip_index"], 1)
            return (r["clip_index"], 0)

        clip_info = sorted(_non_null, key=_interleaved_key)

    # v701n — log final concatenation order so Render confirms the fix
    print(
        f"[Export/v701n] concat order: "
        + " → ".join(
            f"#{r['clip_index']}({(r.get('clip_role') or 'single')[:3]})"
            for r in clip_info
        ),
        flush=True,
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
            # === v698A Phase 4b — DETECT clip-pair scenes ===
            #
            # If any Clip has clip_role='visual_pair', run the dual-output
            # flow: speaker_video (single + audio_pair clips, full Whisper-VAD)
            # AND broll_video (single + visual_pair speed-matched against
            # audio_pair's VAD'd audio + text_card scenes).
            #
            # When no pairs present, fall through to the legacy single-output
            # process_export path (existing behavior, zero breakage on
            # non-v698A jobs).
            _has_v698a_pairs = False
            try:
                _vp_count = db.query(Clip).filter(
                    Clip.job_id == job_id,
                    Clip.clip_role == "visual_pair",
                ).count()
                _has_v698a_pairs = _vp_count > 0
                print(
                    f"[Export/v698A] visual_pair clips in job: {_vp_count}",
                    flush=True,
                )
            except Exception as _v698_err:
                print(
                    f"[Export/v698A] detection failed (non-fatal): {_v698_err}",
                    flush=True,
                )
                _has_v698a_pairs = False

            if _has_v698a_pairs:
                # === DUAL-OUTPUT MODE ===
                # Phase 4b-MVP: orchestrate two process_export passes.
                # The current pass runs the SPEAKER pipeline using a
                # filtered clip_info (drop visual_pair, keep single +
                # audio_pair). After that completes, Phase 4b runs the
                # BROLL pipeline with visual_pair clips pre-swapped to
                # the audio_pair's VAD'd audio.
                #
                # Phase 4b-MVP NOTE: this block currently runs ONLY the
                # speaker pipeline as a first slice. Phase 4b-ii ships the
                # broll pipeline. The legacy single-output stays unaffected
                # for non-v698A jobs.
                print(
                    f"[Export/v698A] DUAL-OUTPUT MODE — "
                    f"speaker pipeline (single + audio_pair) running first",
                    flush=True,
                )
                # Filter clip_info: drop visual_pair entries (their visuals
                # aren't part of the speaker output; their audio twins are
                # already in clip_info as audio_pair entries).
                _speaker_clip_info = [
                    c for c in clip_info
                    if (c.get("clip_role") or "single").lower() != "visual_pair"
                ]
                print(
                    f"[Export/v698A] speaker clip_info: "
                    f"{len(_speaker_clip_info)} clips (filtered from "
                    f"{len(clip_info)} total — {len(clip_info) - len(_speaker_clip_info)} "
                    f"visual_pair entries dropped)",
                    flush=True,
                )

                # The speaker output uses the standard output_path.
                # Phase 4b-ii will add a second output_path for broll.
                _speaker_output_path = output_path

                stats = await asyncio.to_thread(
                    process_export,
                    clip_info=_speaker_clip_info,
                    output_path=_speaker_output_path,
                    frames_to_cut_start=settings.frames_to_cut_start,
                    frames_to_cut_end=settings.frames_to_cut_end,
                    remove_silence=settings.remove_silence,
                    silence_mode=settings.silence_mode,
                    vad_threshold=settings.silence_threshold,
                    silence_trigger=settings.silence_trigger,
                    silence_keep=settings.silence_keep,
                    transition=settings.transition,
                    transition_duration=settings.transition_duration,
                    dialogue_texts=[
                        c.get("dialogue_text", "") or "" for c in _speaker_clip_info
                    ],
                    language=(
                        json.loads(job.config_json).get("language", "English")
                        if job.config_json else "English"
                    ),
                    cut_prefix_audio=False,
                    prefix_word=_prefix_word,
                )
                stats = stats or {}
                stats["v698a_mode"] = "dual_output"
                stats["v698a_speaker_clips"] = len(_speaker_clip_info)
                stats["v698a_visual_pair_clips_in_input"] = (
                    len(clip_info) - len(_speaker_clip_info)
                )
                print(
                    f"[Export/v698A] speaker pipeline complete. Stats: {stats}",
                    flush=True,
                )

                # === Phase 4b-ii — BROLL PIPELINE (v701z) ===
                # Master-audio-alignment restored. Speaker's per-clip
                # Whisper-tiny pass (v701y) disposes its model BEFORE
                # process_export returns; we add an explicit malloc_trim
                # here so RSS is back to baseline before loading the
                # master-audio Whisper-tiny (v701z) for the alignment pass.
                # That sequencing — not overlapping Whisper loads — was
                # what triggered the v701r → OOM regression.
                #
                # Speaker output's audio IS the broll master timeline.
                # Each broll visual is placed at the timestamp where ITS
                # dialogue line plays in master audio:
                #   - 'single'      → dialogue = clip.dialogue_text
                #   - 'visual_pair' → dialogue = clip.voiceover_line
                #   - 'audio_pair'  → SKIP (face-anchor visual, not in broll)
                #   - 'text_card'   → SKIP (no dialogue to align; gap → black)
                # Visuals get speed-adjusted (up to 2× cap) to fit each
                # line's master span; gaps with no paired clip render as
                # BLACK frames. Out-of-budget visuals get speed-cap +
                # trim (process_clip_for_alignment).
                try:
                    # v701z — explicit malloc_trim between speaker and broll
                    # pipelines. Speaker's Whisper-tiny (v701y) was disposed
                    # at the end of its per-clip loop, but glibc may still
                    # hold the freed pages. Trim now so the master-audio
                    # Whisper load below starts from clean RSS.
                    try:
                        import ctypes as _ct
                        _ct.CDLL("libc.so.6").malloc_trim(0)
                        print(
                            "[Export/v698A/broll] v701z malloc_trim applied "
                            "before master-audio pipeline",
                            flush=True,
                        )
                    except Exception:
                        pass

                    from video_processor import export_with_master_audio
                    import tempfile as _tmp

                    def _rehydrate_path(c):
                        p = Path(c.get("path") or "")
                        if not p or p.exists():
                            return
                        if (c.get("scene_type") or "").lower() == "text_card":
                            return
                        if storage is None:
                            return
                        try:
                            r2_key = f"jobs/{job_id}/outputs/{p.name}"
                            if storage.exists(r2_key):
                                print(
                                    f"[Export/v698A/broll] rehydrating from R2: {p.name}",
                                    flush=True,
                                )
                                storage.download_file(r2_key, str(p))
                        except Exception as _rh_err:
                            print(
                                f"[Export/v698A/broll] rehydrate failed for "
                                f"{p.name}: {_rh_err}",
                                flush=True,
                            )

                    # v701zf — broll includes ONLY visual_pair clips.
                    # HOOK + CTA (singles) are persona on-camera → those
                    # windows on the master timeline render as black in
                    # broll (audio plays, no replacement visual). audio_pair
                    # + text_card stay skipped. visual_pair without a
                    # resolvable paired_clip_id (no audio_pair sibling in
                    # speaker) is also skipped — "don't include the clip
                    # if the clip is not paired".
                    broll_clip_info: List[Dict[str, Any]] = []
                    broll_dialogue_lines: List[str] = []
                    for c in clip_info:
                        role = (c.get("clip_role") or "single").lower()
                        if role != "visual_pair":
                            continue  # singles + audio_pair + everything else
                        paired_id = c.get("paired_clip_id")
                        if not paired_id:
                            print(
                                f"[Export/v698A/broll] visual_pair clip "
                                f"{c.get('clip_index')} has no paired_clip_id; "
                                f"skipping (master window stays black)",
                                flush=True,
                            )
                            continue
                        line = (c.get("voiceover_line") or "").strip()
                        if not line:
                            print(
                                f"[Export/v698A/broll] visual_pair clip "
                                f"{c.get('clip_index')} missing voiceover_line; "
                                f"skipping",
                                flush=True,
                            )
                            continue
                        _rehydrate_path(c)
                        broll_clip_info.append(dict(c))
                        broll_dialogue_lines.append(line)

                    # Extract master audio from the freshly-written speaker MP4.
                    # v773.10.18 — when a global speed pass will be applied to
                    # the speaker output later in this function, PRE-APPLY the
                    # same atempo to the master audio we extract for the b-roll
                    # pipeline. Otherwise the b-roll renders against a
                    # 61.16 s audio while the final speaker file is 55.60 s,
                    # leaving the b-roll the wrong length AND out of sync at
                    # every clip boundary. atempo here matches the speed pass
                    # below (line ~8121) exactly: same value, audio-only path.
                    broll_temp_dir = Path(_tmp.mkdtemp(prefix="v698a_broll_"))
                    speaker_master_audio = broll_temp_dir / "speaker_master.mp3"
                    import subprocess as _sp
                    _broll_speed = 1.0
                    if (
                        settings.playback_speed
                        and settings.playback_speed > 1.01
                        and not settings.master_audio_filename
                    ):
                        _broll_speed = round(float(settings.playback_speed), 3)
                    if _broll_speed > 1.01:
                        _audio_cmd = [
                            "ffmpeg", "-y", "-i", str(output_path),
                            "-vn",
                            "-filter:a", f"atempo={_broll_speed:.6f}",
                            "-acodec", "libmp3lame", "-q:a", "2",
                            str(speaker_master_audio),
                        ]
                    else:
                        _audio_cmd = [
                            "ffmpeg", "-y", "-i", str(output_path),
                            "-vn", "-acodec", "libmp3lame", "-q:a", "2",
                            str(speaker_master_audio),
                        ]
                    _audio_res = await asyncio.to_thread(
                        _sp.run, _audio_cmd, capture_output=True, text=True,
                    )
                    if _audio_res.returncode != 0 or not speaker_master_audio.exists():
                        raise RuntimeError(
                            f"speaker audio extraction failed: rc="
                            f"{_audio_res.returncode} stderr={_audio_res.stderr[:300]}"
                        )
                    print(
                        f"[Export/v698A/broll] extracted speaker master audio: "
                        f"{speaker_master_audio.name} "
                        f"({speaker_master_audio.stat().st_size // 1024}KB)",
                        flush=True,
                    )

                    broll_filename = output_filename.replace(
                        "final_export_", "final_broll_"
                    )
                    if broll_filename == output_filename:
                        broll_filename = f"final_broll_{output_filename}"
                    broll_output_path = output_dir / broll_filename

                    # v701zd — build pre-computed targets from speaker's
                    # per-clip post-VAD durations. Speaker pipeline already
                    # trimmed each clip via Whisper-VAD; the resulting
                    # files_to_concat durations ARE the master timeline
                    # positions. No second Whisper master transcription
                    # needed (the legacy path repeatedly under-transcribed
                    # to ~half the script words and bricked alignment).
                    _pre_targets = None
                    _speaker_durs = stats.get("per_clip_post_vad_durations") or []
                    _speaker_db_ids = stats.get("per_clip_post_vad_clip_db_ids") or []
                    if _speaker_durs and _speaker_db_ids:
                        # Build map: speaker clip's db_id → (master_start, master_end)
                        _pos_by_db_id = {}
                        _cursor = 0.0
                        for _i, _d in enumerate(_speaker_durs):
                            _start = _cursor
                            _end = _cursor + _d
                            _cursor = _end
                            _db_id = _speaker_db_ids[_i] if _i < len(_speaker_db_ids) else None
                            if _db_id is not None:
                                _pos_by_db_id[_db_id] = (_start, _end)

                        # For each broll clip, find its position:
                        #   - single (HOOK/CTA): match by own clip_db_id
                        #   - visual_pair: match by paired_clip_id (the audio_pair sibling
                        #     was in speaker concat at that position)
                        _pre_targets = []
                        _all_mapped = True
                        for _bc in broll_clip_info:
                            _role = (_bc.get("clip_role") or "single").lower()
                            if _role == "visual_pair":
                                _lookup_id = _bc.get("paired_clip_id")
                            else:
                                _lookup_id = _bc.get("_clip_db_id")
                            _pos = _pos_by_db_id.get(_lookup_id)
                            if _pos is None:
                                _all_mapped = False
                                print(
                                    f"[Export/v698A/broll] v701zd no speaker position "
                                    f"for broll clip clip_index={_bc.get('clip_index')} "
                                    f"role={_role} lookup_id={_lookup_id} — falling back "
                                    f"to Whisper-master path",
                                    flush=True,
                                )
                                break
                            _start, _end = _pos
                            _pre_targets.append({
                                "start": _start,
                                "end": _end,
                                "target_duration": _end - _start,
                                "confidence": 1.0,
                            })
                        if not _all_mapped:
                            _pre_targets = None
                        else:
                            # v773.10.18 — scale targets to post-speed timeline
                            # so each b-roll slot matches what the speaker will
                            # be after its atempo pass. With _broll_speed=1.1,
                            # a 7.73 s slot becomes 7.03 s and the b-roll clip
                            # gets compressed correspondingly (~1.14× total).
                            if _broll_speed > 1.01:
                                _inv = 1.0 / _broll_speed
                                for _t in _pre_targets:
                                    _t["start"] = _t["start"] * _inv
                                    _t["end"] = _t["end"] * _inv
                                    _t["target_duration"] = _t["end"] - _t["start"]
                            print(
                                f"[Export/v698A/broll] v701zd pre-computed targets built "
                                f"from speaker per-clip durations ({len(_pre_targets)} clips, "
                                f"scaled by 1/{_broll_speed:.3f} for post-speed master)",
                                flush=True,
                            )

                    print(
                        f"[Export/v698A/broll] master-audio alignment: "
                        f"{len(broll_clip_info)} visuals against speaker master → "
                        f"{broll_filename}"
                        + (" (pre-computed targets)" if _pre_targets else " (Whisper master)"),
                        flush=True,
                    )

                    broll_stats = await asyncio.to_thread(
                        export_with_master_audio,
                        clip_info=broll_clip_info,
                        dialogue_lines=broll_dialogue_lines,
                        master_audio_path=speaker_master_audio,
                        output_path=broll_output_path,
                        frames_to_cut_start=0,
                        frames_to_cut_end=0,
                        transition=settings.transition,
                        transition_duration=settings.transition_duration,
                        max_clip_speed=2.0,         # visual_pair clips need ≤2x
                        min_gap_for_black=1.0,      # gaps ≥1s → black; smaller → extend prev clip
                        sequential_alignment=True,  # v701t — fallback path uses sequential matching
                        pre_computed_targets=_pre_targets,  # v701zd
                    )
                    stats["v698a_broll_filename"] = broll_filename
                    stats["v698a_broll_clips"] = len(broll_clip_info)
                    stats["v698a_broll_mode"] = "master_audio_alignment_v701z"
                    stats["v698a_broll_stats"] = broll_stats
                    print(
                        f"[Export/v698A/broll] broll pipeline complete. "
                        f"final_broll → {broll_output_path}",
                        flush=True,
                    )

                    # Upload broll to R2 (if configured)
                    try:
                        from backends.storage import is_storage_configured, get_storage as _gs2
                        if is_storage_configured():
                            _storage = _gs2()
                            _r2_key = f"jobs/{job_id}/outputs/{broll_filename}"
                            await asyncio.to_thread(
                                _storage.upload_file,
                                str(broll_output_path),
                                _r2_key,
                                'video/mp4',
                            )
                            print(
                                f"[Export/v698A/broll] Uploaded to R2: {_r2_key}",
                                flush=True,
                            )
                    except Exception as _r2_err:
                        print(
                            f"[Export/v698A/broll] R2 upload failed (non-fatal): "
                            f"{_r2_err}",
                            flush=True,
                        )
                except Exception as _broll_err:
                    print(
                        f"[Export/v698A/broll] broll pipeline FAILED (non-fatal): "
                        f"{_broll_err}",
                        flush=True,
                    )
                    import traceback as _tb_broll
                    _tb_broll.print_exc()
                    stats["v698a_broll_error"] = str(_broll_err)[:500]
            else:
                # === Regular Export (no master audio, no v698A pairs) ===
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
                    dialogue_texts=[c.get("dialogue_text", "") or "" for c in clip_info],
                    language=json.loads(job.config_json).get("language", "English") if job.config_json else "English",
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
        # v692b — skip speed-apply when concat output is unexpectedly huge.
        # Re-encoding a 233s file under capture_output=True buffers MBs of
        # ffmpeg stderr in Python and OOMs the worker. Cap at 90s; anything
        # larger means concat already broke and speed-apply would crash the
        # container before the file could land in R2. The unsped file still
        # uploads to R2 below so the diagnostic + the partial result reach
        # the user.
        _final_dur_safe = float(stats.get("final_duration") or 0.0)
        _speed_safe = (
            settings.playback_speed and settings.playback_speed > 1.01
            and not settings.master_audio_filename
            and _final_dur_safe > 0
            and _final_dur_safe <= 90.0
        )
        print(
            f"[Export] Speed check: playback_speed={settings.playback_speed}, "
            f"master_audio={settings.master_audio_filename}, "
            f"final_duration={_final_dur_safe:.2f}s, will_apply={_speed_safe}",
            flush=True,
        )
        if not _speed_safe and _final_dur_safe > 90.0:
            print(
                f"[Export][v692b] SKIPPING speed-apply: final_duration "
                f"{_final_dur_safe:.2f}s > 90s. Re-encoding such a large "
                f"file with capture_output=True risks OOM. Inspect "
                f"[VideoProcessor/v692b] pre-concat / post-concat lines to "
                f"localize the bloat. Unsped file still uploads to R2.",
                flush=True,
            )
        if _speed_safe:
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

        # Mark job as exported (v776: also enter post-render lifecycle).
        job.has_export = True
        # v783 (2026-06-05): export only runs on approved+ready clips. If
        # job.status is stuck at 'processing'/'running'/'pending' (e.g. an
        # earlier Flow redo reverted status — main.py L12816 — and the
        # auto-flip in /api/user-worker/clips/{id}/status didn't fire,
        # leaving the badge stuck at PROCESSING forever), force-flip to
        # completed here. Export running == job is done. Never override
        # cancelled / failed (operator-intent terminal states).
        if job.status not in (JobStatus.CANCELLED.value, JobStatus.FAILED.value):
            job.status = JobStatus.COMPLETED.value
            if job.completed_at is None:
                job.completed_at = datetime.utcnow()
        _maybe_auto_enter_lifecycle(job, now=datetime.utcnow())
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
    
    # Cache headers — frame filenames are unique + never mutate, so cache
    # for 365d (v755: was 1h; the short TTL forced a full re-download of
    # every frame through the 1-CPU origin once an hour / each new session).
    cache_headers = {"Cache-Control": "public, max-age=31536000, immutable"}
    
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
                # v695 — REDIRECT-TO-R2 PATH REMOVED ENTIRELY. v687 made it
                # opt-in via ?direct=1; presigned R2 URLs continued to leak
                # in cached responses. Only behavior now: download from R2
                # → cache → return bytes. Browser never sees R2 host.
                print(
                    f"[Images/v695] cold-cache miss: r2_key={r2_key} — "
                    f"downloading from R2 to local then streaming bytes "
                    f"(no redirect)",
                    flush=True,
                )
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
        # Deploy probe: which commit this container was built from. Render
        # sets RENDER_GIT_COMMIT at build time. Unauthenticated on purpose —
        # lets us confirm a deploy landed without dashboard access.
        "render_commit": os.environ.get("RENDER_GIT_COMMIT", "not set"),
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


# v701 — policy-violation worker endpoint. Defined here (rather than next
# to the user-auth /replace-image endpoint at ~line 3525) because
# `verify_local_worker_key` is declared above. Putting the Depends call
# above the def-site causes NameError at module load (Python evaluates
# default arg values at function-definition time).
@app.post("/api/local-worker/clips/{clip_id}/policy-violation")
async def local_worker_report_policy_violation(
    clip_id: int,
    request: PolicyViolationRequest,
    authorized: bool = Depends(verify_local_worker_key),
):
    """v701 — Worker reports that Flow rejected the clip's start_frame on
    content-policy grounds. Backend stamps error_code = CONTENT_POLICY_VIOLATION
    and stashes the rejected R2 key so the frontend can render the
    replace-image card. Status stays 'failed' so the existing review
    banner counts it correctly; the UI branches on error_code."""
    from models import get_db
    db = next(get_db())
    try:
        clip = db.query(Clip).filter(Clip.id == clip_id).first()
        if not clip:
            raise HTTPException(status_code=404, detail="Clip not found")

        # Stash the rejected frame for audit + UI render. Prefer the worker-
        # supplied key; fall back to whatever start_frame held at violation
        # time (which IS the offending frame by definition).
        rejected_key = (
            request.rejected_image_key.strip()
            if request.rejected_image_key
            else (clip.start_frame or "").strip()
        )
        if rejected_key:
            clip.replacement_start_frame = rejected_key

        clip.status = ClipStatus.FAILED.value
        clip.error_code = "CONTENT_POLICY_VIOLATION"
        clip.error_message = (
            request.detail
            or "⚠️ Flow rejected this image's content. Upload a replacement to retry."
        )
        db.commit()

        # v701e — preemptive image-shared cascade.
        # Once Flow rejects an image on policy, every OTHER pending /
        # generating / redo-queued clip in the same job using the SAME
        # start_frame is going to fail too. Mark them all as awaiting-
        # replacement now so the worker stops wasting cycles + Veo
        # credits retrying the same flagged image. Once the user uploads
        # a replacement on ANY ONE of them, the v701d cascade in
        # /replace-image patches the siblings back to pending.
        cascaded_marked = 0
        try:
            if rejected_key:
                # Statuses worth preempting: anything that hasn't completed
                # successfully and isn't already stamped with a different
                # error_code (don't overwrite a non-policy failure).
                sibling_q = db.query(Clip).filter(
                    Clip.job_id == clip.job_id,
                    Clip.start_frame == rejected_key,
                    Clip.id != clip.id,
                    Clip.status.in_([
                        ClipStatus.PENDING.value,
                        ClipStatus.GENERATING.value,
                        ClipStatus.REDO_QUEUED.value,
                        ClipStatus.FLOW_REDO_QUEUED.value,
                        ClipStatus.FAILED.value,
                    ]),
                )
                for sib in sibling_q.all():
                    # Status filter above explicitly excludes COMPLETED so
                    # already-rendered siblings are never touched by the
                    # cascade (rendered b-roll is the user's truth, not
                    # something to clobber). Don't clobber a non-policy
                    # error_code (e.g. CELEBRITY_FILTER) either.
                    if sib.error_code and sib.error_code != "CONTENT_POLICY_VIOLATION":
                        continue
                    sib.status = ClipStatus.FAILED.value
                    sib.error_code = "CONTENT_POLICY_VIOLATION"
                    sib.error_message = (
                        "⚠️ Flow rejected this image's content (cascade from sibling). "
                        "Upload a replacement to retry."
                    )
                    sib.replacement_start_frame = sib.replacement_start_frame or rejected_key
                    sib.claimed_by_worker = None
                    sib.claimed_at = None
                    cascaded_marked += 1
                if cascaded_marked:
                    db.commit()
        except Exception as _cascade_err:
            # v701-cleanup — full traceback so silent cascade failures
            # are visible (cavecrew finding).
            import traceback
            print(
                f"[v701e] preemptive cascade FAILED for clip {clip_id}: "
                f"{type(_cascade_err).__name__}: {_cascade_err}",
                flush=True,
            )
            traceback.print_exc()
            db.rollback()

        cascade_msg = (
            f" (preemptively marked {cascaded_marked} sibling"
            f"{'s' if cascaded_marked != 1 else ''} sharing same image)"
            if cascaded_marked else ""
        )
        add_job_log(
            db, clip.job_id,
            f"Clip {clip.clip_index + 1}: image policy violation — awaiting user replacement{cascade_msg}",
            "WARNING",
            "policy",
        )
        db.commit()

        return {
            "ok": True,
            "clip_id": clip_id,
            "rejected_image_key": rejected_key or None,
            "cascaded_sibling_count": cascaded_marked,  # v701e
        }
    finally:
        db.close()


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
            "clip_mode": clip.clip_mode or "fresh",
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
            "veo_model": config.get("veo_model", "Veo 3.1 - Lite [Lower Priority]"),
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
                    clip_mode = clip.clip_mode or "fresh"
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
            "clip_mode": clip.clip_mode or "fresh",
            "scene_index": clip.scene_index or 0,
            "short_dialogue_mode": job_config.get("short_dialogue_mode", "optimized"),
            "prefix_short_enabled": job_config.get("prefix_short_enabled", False),
            "prefix_short_word": job_config.get("prefix_short_word", "only"),
            "prefix_short_threshold": job_config.get("prefix_short_threshold", 15),
            "flow_variants_count": job_config.get("flow_variants_count", 2),
            "veo_model": job_config.get("veo_model", "Veo 3.1 - Lite [Lower Priority]"),
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
    # v701-cleanup — let the worker stamp error_code via the generic
    # status endpoint. Used as a fallback when the dedicated
    # /policy-violation endpoint is unavailable (404 mid-rollout) so
    # the frontend still sees CONTENT_POLICY_VIOLATION and renders the
    # upload-replacement card. Optional to preserve backwards-compat.
    error_code: Optional[str] = None


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
        # v698A — audio_pair clips are NOT user-reviewable as visuals
        # (their visual is discarded at export; only their audio is used).
        # Auto-approve them on completion so the export query picks them
        # up alongside single + visual_pair clips.
        if update.status == 'completed' and (clip.clip_role or '').lower() == 'audio_pair':
            clip.approval_status = 'approved'
            print(
                f"[v698A] audio_pair clip {clip.id} auto-approved "
                f"on completion (paired with visual_pair {clip.paired_clip_id})",
                flush=True,
            )
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
    if update.error_code:  # v701-cleanup
        clip.error_code = update.error_code
    
    # v761f — clear stale redo pre-set rejection on completion. The redo
    # flow sets approval_status='rejected' (old variant shows rejected
    # while regenerating); the gated reset below only fires when old_status
    # is still in the redo/generating set. If an intermediate or duplicate
    # status update knocked clip.status off that set before this 'completed'
    # landed, the reset was skipped and the freshly-rendered clip stayed
    # stuck on 'rejected' (good video, REJECTED label, no approve button).
    # A worker 'completed' report always means a fresh render finished —
    # never a deliberate user rejection — so clear a stale 'rejected'.
    # 'approved' is left untouched (preserved across completed re-reports).
    if update.status == 'completed' and clip.approval_status == 'rejected':
        print(f"[v761f] local-worker: clip {clip.id} completed with stale approval=rejected (old_status={old_status}) — reset to pending_review", flush=True)
        clip.approval_status = 'pending_review'

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
        "status": clip.status,
        # v701i — surface error_code so the worker's pre-submit check can
        # skip clips that were preemptively marked CONTENT_POLICY_VIOLATION
        # by a sibling's policy report. Without this the worker keeps
        # clicking Generate on flagged-image clips that are guaranteed
        # to fail again, burning Veo credits + Flow rate-limit budget.
        "error_code": clip.error_code,
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

        # v693 — store the BACKEND PROXY URL (not the presigned R2 URL).
        # The proxy endpoint /api/jobs/{job_id}/outputs/{filename} streams
        # bytes through the app server via FileResponse (with R2 download
        # fallback) so the user's browser never has to reach
        # *.r2.cloudflarestorage.com directly. Pre-v693 we stored
        # `storage.get_presigned_url(r2_key, expires_in=86400*7)` here,
        # which baked in a 7-day-valid R2 host URL. The frontend then used
        # versions[].url verbatim for downloads (downloadClip in
        # static/index.html) and the browser hit R2 → ERR_CONNECTION_TIMED_OUT
        # for any user whose ISP/firewall blocks cloudflarestorage.com.
        # Same fix v687 already applied to thumbnails.
        output_filename = f"clip_{clip_index}_{attempt}.{variant}.mp4"
        output_url = f"/api/jobs/{job_id}/outputs/{output_filename}"
        
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


@app.post("/api/user-worker/heartbeat")
async def user_worker_heartbeat(
    request: Request,
    authorization: str = Header(None),
    db: DBSession = Depends(get_db_session),
):
    """v780 — dedicated heartbeat for the video (Flow) worker.

    The My Worker page's online indicator keys off ``token.last_seen``. The
    generic ``verify_user_worker_token`` dependency throttles last_seen writes
    to once per 60s (connection-pool protection) — too coarse for a responsive
    indicator and the reason the dot lagged ~30s. This endpoint writes
    last_seen UNCONDITIONALLY on every call. The worker pings every 5s, so a
    live worker's last_seen never ages past ~5s and the status endpoint's 15s
    stale window flips Offline within ~5-15s of an unclean death.

    ``going_offline=true`` (clean Ctrl+C / atexit) backdates last_seen beyond
    the window so the UI flips to Offline on its very next poll (~3s).
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

    going_offline = False
    try:
        body = await request.json()
        going_offline = bool(body.get("going_offline"))
    except Exception:
        going_offline = False

    now = datetime.utcnow()
    if going_offline:
        # Backdate beyond the 15s window — UI sees Offline on next poll.
        token.last_seen = now - timedelta(seconds=3600)
        # v780 diagnostic — clean-stop signal landed. Low frequency (once per
        # worker shutdown), so it doesn't spam. Remove once evidence confirms.
        print(f"[v780] user-worker going_offline user={token.user_id}", flush=True)
    else:
        token.last_seen = now
    db.commit()
    return {"ok": True, "going_offline": going_offline}


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
                    if (clip.clip_mode or "fresh") == "blend":
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
            "clip_mode": clip.clip_mode or "fresh",
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
            "veo_model": job_config.get("veo_model", "Veo 3.1 - Lite [Lower Priority]"),
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
                    clip_mode = clip.clip_mode or "fresh"
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
            "clip_mode": clip.clip_mode or "fresh",
            "scene_index": clip.scene_index or 0,
            "short_dialogue_mode": job_config.get("short_dialogue_mode", "optimized"),
            "prefix_short_enabled": job_config.get("prefix_short_enabled", False),
            "prefix_short_word": job_config.get("prefix_short_word", "only"),
            "prefix_short_threshold": job_config.get("prefix_short_threshold", 15),
            "flow_variants_count": job_config.get("flow_variants_count", 2),
            "veo_model": job_config.get("veo_model", "Veo 3.1 - Lite [Lower Priority]"),
        })
    
    return {"clips": clips_data}


@app.get("/api/user-worker/clips/kling-pending")
async def user_worker_get_kling_clips(
    request: Request,
    worker_id: Optional[str] = Query(None),
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token),
):
    """Clips queued for a Kling (Higgsfield) variant, for THIS user's jobs.
    The local worker generates each via the `higgsfield` CLI (Kling 3.0 + audio,
    residential IP) and uploads via
    /api/user-worker/jobs/{job_id}/upload-video/{clip_index}.
    """
    import json as _json
    import os as _os
    from datetime import timedelta as _td

    # User's job ids (used to scope clip queries without an UPDATE..JOIN,
    # which SQLAlchemy ORM .update() does not support).
    user_job_ids = [row[0] for row in db.query(Job.id).filter(Job.user_id == user_id).all()]
    if not user_job_ids:
        return {"clips": []}

    # Release stale claims (>15 min stuck in 'processing').
    stale_cut = datetime.utcnow() - _td(minutes=15)
    db.query(Clip).filter(
        Clip.job_id.in_(user_job_ids),
        Clip.kling_variant_status == 'processing',
        Clip.claimed_at < stale_cut,
    ).update({Clip.kling_variant_status: 'queued'}, synchronize_session=False)
    db.commit()

    clips = db.query(Clip).filter(
        Clip.job_id.in_(user_job_ids),
        Clip.kling_variant_status == 'queued',
    ).order_by(Clip.id.asc()).limit(5).all()

    base_url = str(request.base_url).rstrip('/')
    out = []
    for clip in clips:
        job = db.query(Job).filter(Job.id == clip.job_id).first()
        if not job:
            continue
        try:
            frames_map = _json.loads(job.frames_storage_keys) if job.frames_storage_keys else {}
        except Exception:
            frames_map = {}
        frames_list = sorted(frames_map.keys())
        # Resolve the start-frame filename (basename of clip.start_frame, else by start_image_idx).
        start_filename = _os.path.basename(str(clip.start_frame)) if clip.start_frame else None
        idx = None
        try:
            data = _json.loads(job.dialogue_json or "{}")
            for line in (data.get("lines") or []):
                if isinstance(line, dict) and line.get("id") == clip.dialogue_id:
                    idx = line.get("start_image_idx")
                    break
        except Exception:
            idx = None
        if (not start_filename or start_filename not in frames_map) and frames_list:
            i = idx if isinstance(idx, int) else (clip.clip_index % len(frames_list))
            start_filename = frames_list[i % len(frames_list)]
        if not start_filename:
            continue  # frames not ready yet — leave queued

        # Motion prompt: the clip's verbatim Veo prompt override, else the spoken line.
        prompt = clip.dialogue_text or ""
        try:
            data = _json.loads(job.dialogue_json or "{}")
            for line in (data.get("lines") or []):
                if isinstance(line, dict) and line.get("id") == clip.dialogue_id:
                    ov = line.get("veo_prompt_override")
                    if ov and ov.strip():
                        prompt = ov.strip()
                    break
        except Exception:
            pass
        if not (prompt or "").strip():
            prompt = "Subtle natural motion, static locked-off camera."

        duration = 5
        try:
            cfg = _json.loads(job.config_json or "{}")
            duration = int(cfg.get("duration") or 5)
        except Exception:
            duration = 5

        # Claim it so a second poll/worker doesn't double-generate.
        clip.kling_variant_status = 'processing'
        clip.claimed_at = datetime.utcnow()
        if worker_id:
            clip.claimed_by_worker = worker_id

        out.append({
            "clip_id": clip.id,
            "job_id": clip.job_id,
            "clip_index": clip.clip_index,
            "start_frame_url": f"{base_url}/api/user-worker/frames/{clip.job_id}/{start_filename}",
            "prompt": prompt,
            "duration": duration,
        })
    db.commit()
    return {"clips": out}


@app.post("/api/user-worker/clips/{clip_id}/kling-status")
async def user_worker_set_kling_status(
    clip_id: int,
    status: str = Query(...),
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token),
):
    """Worker marks a clip's Kling state. 'failed' is TERMINAL — the kling-pending
    poll never re-serves it, so a give-up (out of credits / NSFW / exhausted
    retries) stops the clip from re-firing forever."""
    if status not in ("queued", "processing", "done", "failed"):
        raise HTTPException(status_code=400, detail="bad status")
    clip = db.query(Clip).join(Job).filter(Clip.id == clip_id, Job.user_id == user_id).first()
    if not clip:
        raise HTTPException(status_code=404, detail="clip not found")
    clip.kling_variant_status = status
    db.commit()
    return {"ok": True, "clip_id": clip_id, "kling_variant_status": status}


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


# v701-prefix-fix — user-worker policy-violation endpoint mirrors the
# local-worker version. report_policy_violation in flow_worker.py uses a
# RELATIVE path (`/clips/{id}/policy-violation`) and api_request_ex
# prepends the active API_PATH_PREFIX (`/api/user-worker` in USER mode,
# `/api/local-worker` in legacy mode). Without this mirror, USER-mode
# workers got 404 → fallback path → no error_code stamped → no upload
# card surfaced.
@app.post("/api/user-worker/clips/{clip_id}/policy-violation")
async def user_worker_report_policy_violation(
    clip_id: int,
    request: PolicyViolationRequest,
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token),
):
    """v701 — User-mode equivalent of local-worker policy-violation.
    Verifies ownership via user_id then runs the same Clip-stamping
    + v701e preemptive image-shared cascade as the local-worker path."""
    clip = db.query(Clip).join(Job).filter(
        Clip.id == clip_id, Job.user_id == user_id
    ).with_for_update().first()
    if not clip:
        raise HTTPException(status_code=404, detail="Clip not found or not yours")

    rejected_key = (
        request.rejected_image_key.strip()
        if request.rejected_image_key
        else (clip.start_frame or "").strip()
    )
    if rejected_key:
        clip.replacement_start_frame = rejected_key

    clip.status = ClipStatus.FAILED.value
    clip.error_code = "CONTENT_POLICY_VIOLATION"
    clip.error_message = (
        request.detail
        or "⚠️ Flow rejected this image's content. Upload a replacement to retry."
    )
    db.commit()

    # v701e preemptive sibling cascade.
    cascaded_marked = 0
    try:
        if rejected_key:
            sibling_q = db.query(Clip).filter(
                Clip.job_id == clip.job_id,
                Clip.start_frame == rejected_key,
                Clip.id != clip.id,
                Clip.status.in_([
                    ClipStatus.PENDING.value,
                    ClipStatus.GENERATING.value,
                    ClipStatus.REDO_QUEUED.value,
                    ClipStatus.FLOW_REDO_QUEUED.value,
                    ClipStatus.FAILED.value,
                ]),
            )
            for sib in sibling_q.all():
                # Status filter above explicitly excludes COMPLETED so
                # already-rendered siblings are never touched. Don't
                # clobber non-policy error_code (e.g. CELEBRITY_FILTER).
                if sib.error_code and sib.error_code != "CONTENT_POLICY_VIOLATION":
                    continue
                sib.status = ClipStatus.FAILED.value
                sib.error_code = "CONTENT_POLICY_VIOLATION"
                sib.error_message = (
                    "⚠️ Flow rejected this image's content (cascade from sibling). "
                    "Upload a replacement to retry."
                )
                sib.replacement_start_frame = sib.replacement_start_frame or rejected_key
                sib.claimed_by_worker = None
                sib.claimed_at = None
                cascaded_marked += 1
            if cascaded_marked:
                db.commit()
    except Exception as _cascade_err:
        # v701-cleanup — log full traceback so silent cascade failures are
        # visible in Render logs (cavecrew flagged the bare except as a
        # hidden trap: user uploads replacement, cascade fails, no error
        # surface).
        import traceback
        print(
            f"[v701e/user-worker] preemptive cascade FAILED for clip {clip_id}: "
            f"{type(_cascade_err).__name__}: {_cascade_err}",
            flush=True,
        )
        traceback.print_exc()
        db.rollback()

    cascade_msg = (
        f" (preemptively marked {cascaded_marked} sibling"
        f"{'s' if cascaded_marked != 1 else ''} sharing same image)"
        if cascaded_marked else ""
    )
    add_job_log(
        db, clip.job_id,
        f"Clip {clip.clip_index + 1}: image policy violation — awaiting user replacement{cascade_msg}",
        "WARNING",
        "policy",
    )
    db.commit()

    return {
        "ok": True,
        "clip_id": clip_id,
        "rejected_image_key": rejected_key or None,
        "cascaded_sibling_count": cascaded_marked,
    }


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
    if update.error_code:  # v701-cleanup
        clip.error_code = update.error_code

    # v761f — clear stale redo pre-set rejection on completion (see the
    # local-worker endpoint for the full rationale). A worker 'completed'
    # report always means a fresh render finished, never a deliberate user
    # rejection, so a 'rejected' approval at this point is the stale redo
    # pre-set and must flip to pending_review. The gated reset below only
    # fires when old_status stayed in the redo/generating set, which an
    # intermediate/duplicate status update can defeat. 'approved' untouched.
    if update.status == 'completed' and clip.approval_status == 'rejected':
        print(f"[v761f] user-worker: clip {clip.id} completed with stale approval=rejected (old_status={old_status}) — reset to pending_review", flush=True)
        clip.approval_status = 'pending_review'

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
        # v773 — surface error_code so the worker's pre-redo guard
        # (flow_worker process_redo_clip) can SKIP clips preemptively marked
        # CONTENT_POLICY_VIOLATION (prominent-people → awaiting user image
        # replacement). The local-worker endpoint already returns this; the
        # user-worker copy was missing it, so in USER MODE the guard was blind
        # and auto-redid flagged clips (with a wrong model swap). Mirror it.
        "error_code": clip.error_code,
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
        # v693 — store backend proxy URL (see LocalWorker upload comment).
        output_filename = f"clip_{clip_index}_{attempt}.{variant}.mp4"
        output_url = f"/api/jobs/{job_id}/outputs/{output_filename}"
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
            
            # Close the Kling-variant loop: a clip queued/processing for a Kling
            # variant is now satisfied by this upload → mark done so the
            # kling-pending poll stops re-serving it. attempt==9 = the Kling
            # marker; mark the clip viewable now so the variant shows even if the
            # Flow/Veo pass for this clip hasn't finished yet.
            if clip.kling_variant_status in ('queued', 'processing'):
                clip.kling_variant_status = 'done'
            if attempt == 9:
                clip.status = ClipStatus.COMPLETED.value
                if clip.approval_status not in ('approved', 'rejected'):
                    clip.approval_status = 'pending_review'
                clip.error_code = None
                clip.error_message = None

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


@app.get("/api/user-worker/download/worker_profile_pull.py")
async def serve_worker_profile_pull():
    """Serve the laptop-profile-pull companion module. flow_worker.py imports it;
    the worker's auto-updater fetches it next to flow_worker.py every launch."""
    mod_path = Path(__file__).parent / "static" / "worker_profile_pull.py"
    if not mod_path.exists():
        raise HTTPException(404, "Module not found")
    return Response(content=mod_path.read_text(), media_type="text/x-python")


@app.get("/api/user-worker/download/worker_cookie_extract.py")
async def serve_worker_cookie_extract():
    """Serve the cookie-extraction companion module (decrypts the operator's own
    Chrome login for injection into the worker's fresh session). Fetched by the
    worker's auto-updater next to flow_worker.py every launch."""
    mod_path = Path(__file__).parent / "static" / "worker_cookie_extract.py"
    if not mod_path.exists():
        raise HTTPException(404, "Module not found")
    return Response(content=mod_path.read_text(), media_type="text/x-python")


@app.get("/api/user-worker/download/flow_attribution.py")
async def serve_flow_attribution():
    """Serve the v800 click-bracket attribution companion module. flow_worker.py
    imports it; the worker's auto-updater fetches it next to flow_worker.py every
    launch. flow_worker's import is resilient (falls back to legacy) if this is
    ever missing, but the updater fetches it so the feature actually runs."""
    mod_path = Path(__file__).parent / "static" / "flow_attribution.py"
    if not mod_path.exists():
        raise HTTPException(404, "Module not found")
    return Response(content=mod_path.read_text(), media_type="text/x-python")


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
            # v780: stale window 30s -> 15s. Safe because the video worker now
            # sends a dedicated /api/user-worker/heartbeat every 5s that writes
            # last_seen UNCONDITIONALLY (bypassing the 60s token-verify throttle),
            # so a live worker never ages past ~5s. Unclean death now flips the
            # UI to Offline within ~5-15s instead of the old ~30s.
            if (datetime.utcnow() - t.last_seen).total_seconds() < 15:
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
    laptop_email: str = Query(""),
    user: User = Depends(get_current_user),
    db: DBSession = Depends(get_db_session)
):
    """Generate OS-specific installer with user's token baked in.

    Settings from the web UI are baked into the installer:
    - accounts: number of Chrome windows
    - reset: 1 = wipe session folders for fresh Google login
    - update_only: 1 = only re-download flow_worker.py, keep everything else
    - laptop_email: reuse the Chrome profile already logged into this Gmail so
      the worker skips the Google verification code (blank = manual login)
    """
    # Sanitize the email before it is baked into a shell .env line.
    _laptop_email = (laptop_email or "").strip()
    if len(_laptop_email) > 254 or not _re.match(
            r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$', _laptop_email):
        _laptop_email = ""
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
        content = _generate_windows_installer(token.id, app_url, accounts, bool(reset), bool(update_only), _laptop_email)
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
        content = _generate_unix_installer(token.id, app_url, accounts, bool(reset), bool(update_only), _laptop_email)
        inner_filename = "KavenoBuilder-Worker-Setup.command"
        
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            import time as _t
            info = zipfile.ZipInfo(inner_filename, date_time=_t.localtime()[:6])
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


def _generate_windows_installer(token: str, app_url: str, accounts: int = 1, reset: bool = False, update_only: bool = False, laptop_email: str = "") -> str:
    """Generate a Windows .bat — simple, one process, everything direct. This is the approach that works."""
    
    # Each account flag needs its OWN `echo` — a multi-line value after a single
    # `echo` only writes line 1; the rest run as bare commands and fail to write.
    env_accounts = "echo ACCOUNT1_ENABLED=true"
    for n in range(2, accounts + 1):
        env_accounts += f"\necho ACCOUNT{n}_ENABLED=true"
    for n in range(accounts + 1, 5):
        env_accounts += f"\necho ACCOUNT{n}_ENABLED=false"

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
echo ACCOUNT1_LAPTOP_EMAIL={laptop_email}
{env_accounts}
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


def _generate_unix_installer(token: str, app_url: str, accounts: int = 1, reset: bool = False, update_only: bool = False, laptop_email: str = "") -> str:
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
ACCOUNT1_LAPTOP_EMAIL={laptop_email}
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