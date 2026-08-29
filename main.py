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
import tempfile
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
from finishing_models import ExportSettings, AutoEditRequest
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
from clip_duration import (
    ALLOWED_CLIP_DURATIONS_S,
    CLIP_CHAR_BUCKETS,
    CLIP_DURATION_BUCKETS,
    VEO_API_DURATIONS_S,
)
from lifecycle import apply_lifecycle_change, compute_stuck_days, apply_jobs_filters, _maybe_auto_enter_lifecycle, derive_effective_stage, _LIFECYCLE_STAGE_TO_TIMESTAMP_FIELD
from auto_image_retry import parse_auto_image_retry_mode, VALID_RETRY_MODES, order_distinct_frames, pick_substitute
from worker import worker, WORKER_VERSION
from error_handler import ErrorCode
from job_age import job_age_cutoff

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
    # v805 — per-clip Prompt B (policy fallback, voice-only). Stored
    # verbatim on the clip (never composed by build_prompt); the flow
    # worker retries a generation-policy-blocked clip with this text on
    # the SAME model before swapping models.
    veo_prompt_b: Optional[str] = None
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
    # v892 / v892.1 — composite background layer. composite_plate_image_node_id
    # is the FK to the plate ImageNode; the _local_index is that image's
    # position in the upload list, which Phase 3b needs to bind the spawned
    # composite_plate Clip's start frame. composite_plate_prompt_override is
    # the operator-authored `### Clip S.L.plate` text. All NULL on every
    # non-composite line.
    # v892.2 — the plate's node id MUST be declared here too. It is what the
    # Clip writer stores as Clip.composite_plate_image_node_id, and Phase 3a
    # refuses to create a plate clip without it. Undeclared, pydantic dropped
    # it out of the payload at job creation and the field reached the Clip row
    # as NULL — the silent middle link in the v892 chain.
    composite_plate_image_node_id: Optional[int] = None
    composite_plate_image_local_index: Optional[int] = None
    composite_plate_prompt_override: Optional[str] = None
    # v718i (NEW 2026-05-18) — Veo native end-frame interpolation binding.
    # When set, veo_generator.py:2605 binds cfg.last_frame to this ImageNode's
    # rendered output instead of auto-inferring from next clip's start image.
    # NULL on every non-Option-C dialogue line (legacy sequential default).
    end_frame_image_node_id: Optional[int] = None
    end_frame_image_local_index: Optional[int] = None
    # v943 — character-swap binding. Declared here for the same reason v892.2
    # had to declare the plate node id: pydantic drops what it is not told
    # about, so an undeclared field reaches the Clip row as NULL and the whole
    # chain looks wired while doing nothing. All four are NULL on every clip
    # that renders the normal way.
    render_method: Optional[str] = None
    swap_source_r2_key: Optional[str] = None
    swap_mode: Optional[str] = None
    swap_avatar_upload_id: Optional[int] = None
    # v943.1 — export-time source audio for a swap clip. Declared here for the
    # same v892.2 reason as the four above: pydantic drops what it is not told
    # about, and an undeclared field reaches the Clip row as NULL while every
    # surface upstream of it looks correctly wired.
    swap_audio: Optional[str] = None


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
    # v861 — adaptive_duration ON (default): each clip renders at the bucket its
    # line's word count lands in (<=11w=4s, 12-16w=6s, 17-24w=8s, 25-28w=10s),
    # resolved at import onto clips.veo_render_duration_s. OFF: every clip
    # renders at `duration` above — the Clip writer stores NULL, and NULL
    # already means "job-level duration" on both render paths, so nothing
    # downstream needs to know about this flag.
    adaptive_duration: bool = True
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


# v931 — redos are unlimited (operator 2026-08-18). attempts_remaining stays
# in the API as a positive sentinel so older frontends' "disable at <= 0"
# guards never fire. 999 is never displayed (v931 UI strips the counters).
UNLIMITED_ATTEMPTS_REMAINING = 999


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
    attempts_remaining: int = UNLIMITED_ATTEMPTS_REMAINING
    redo_reason: Optional[str] = None
    versions: List[Dict] = []
    # Variant fields
    selected_variant: int = 1
    total_variants: int = 0
    # Scene/mode fields
    clip_mode: Optional[str] = "fresh"  # v782 default fresh (was blend)
    scene_index: Optional[int] = 0
    # v861 per-clip render length. These are SET by the promote path and by
    # POST /api/jobs, and PATCHable, but were never returned — so the only way
    # to check what a clip would actually render at was to watch the output and
    # measure it. A duration that silently falls back to the job-wide
    # config.duration looks identical to one that bound correctly until the mp4
    # lands. Returning them makes the binding checkable before the render, not
    # after (2026-08-18: three batches rendered 8s each before this was visible).
    target_duration_s: Optional[float] = None
    veo_render_duration_s: Optional[int] = None
    # Prompt
    prompt_text: Optional[str] = None
    # v805/v821 — Prompt B policy fallback + its reworded line + active variant
    prompt_text_b: Optional[str] = None
    dialogue_text_b: Optional[str] = None
    rendered_prompt_variant: Optional[str] = "A"
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
    # v815 — auto-image-retry audit: {original_frame, used_frame, tried, count, mode}
    auto_image_retry: Optional[Dict[str, Any]] = None
    # v939 — shadow-mode clip QC: did each rendered take say its whole line?
    # None until code/clip_qc.py scores it. Advisory only; chooses nothing.
    qc: Optional[Dict[str, Any]] = None


class ClipQCRequest(BaseModel):
    report: Dict[str, Any]


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
    #
    # v872 — the memory cost of a large pool is now bounded by MALLOC_ARENA_MAX=2
    # (Dockerfile): glibc's default is one arena PER THREAD, so 256 threads used
    # to mean up to 256 heaps each hoarding their own freed pages. With the cap
    # in place the pool size no longer drives RSS, so 256 stays — it is a
    # deliberate playback fix, not an accident. Tunable if that ever changes.
    try:
        import anyio
        _tokens = int(os.environ.get("ANYIO_THREAD_TOKENS", "256"))
        anyio.to_thread.current_default_thread_limiter().total_tokens = _tokens
        print(f"[startup][v75x] anyio thread limiter total_tokens={_tokens} (video stream relief)", flush=True)
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

        # v931 — one-time normalization: clips stranded 'max_attempts' by the
        # retired 3-attempt redo cap become reviewable again. Idempotent.
        try:
            from models import Clip as _Clip931, get_db as _get_db931
            def _clear_max_attempts():
                with _get_db931() as _db:
                    _n = _db.query(_Clip931).filter(_Clip931.approval_status == "max_attempts").update(
                        {"approval_status": "pending_review"}, synchronize_session=False
                    )
                    if _n:
                        _db.commit()
                    return _n
            _nm = await _asyncio.to_thread(_clear_max_attempts)
            if _nm:
                print(f"[Deferred][v931] Cleared legacy max_attempts flag on {_nm} clip(s) — redos are unlimited now", flush=True)
        except Exception as _v931e:
            print(f"[Deferred][v931] max_attempts normalization failed: {_v931e}", flush=True)

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

        # v850 — re-run exports the last container was killed mid-way through
        # (Render deploy / OOM). Their heartbeat is stale (or NULLed by the
        # graceful shutdown hook), so the sweep reclaims them right now instead
        # of the operator staring at a poll that will never resolve.
        try:
            # v855 — the sweep only RE-QUEUES; the dispatcher starts them, one at
            # a time. Spawning them all here (as v850 did) fired every orphan at
            # once on a 2 GB box — OOM, restart, repeat.
            _orphan_ids = await _asyncio.to_thread(_sweep_stale_exports)
            if _orphan_ids:
                print(f"[Deferred][Export/v850] re-queued {len(_orphan_ids)} export(s) orphaned by the last restart", flush=True)
            else:
                print("[Deferred][Export/v850] no orphaned exports to re-run", flush=True)
        except Exception as _xe:
            print(f"[Deferred][Export/v850] boot sweep: {_xe}", flush=True)

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

    # v866 — continuous memory sampler. The 2026-07-23 OOMs left no trace of
    # WHAT allocated: the log simply stopped mid-export and the instance
    # restarted. This samples the cgroup on a daemon THREAD (an asyncio task
    # would not be scheduled while sync ffmpeg/whisper work blocks the loop)
    # and speaks up past 60% of the limit, carrying a phase label and the
    # high-water mark, so the last line before a kill is actually informative.
    try:
        import mem_guard as _mg866
        _mg866.start_sampler(interval=3.0)
    except Exception as _e866:
        print(f"[Mem/v866] sampler failed to start (non-fatal): {_e866}", flush=True)

    # v850 — durable export queue: re-fire anything a dead container orphaned.
    # Covers the hard-kill path (OOM/SIGKILL) where the shutdown hook below
    # never ran.
    _export_dispatcher_task = _asyncio.create_task(_export_dispatcher())
    print(f"[App][Export/v855] export dispatcher started "
          f"(max {_eq.MAX_CONCURRENT} concurrent, {_eq.DISPATCH_INTERVAL_S}s tick)", flush=True)
    _export_sweeper_task = _asyncio.create_task(_export_sweeper())
    print("[App][Export/v850] stale-export sweeper started (every 60s)", flush=True)

    # v938 — server-side auto-edit. The local worker still wins any run it is
    # awake for (see AUTOEDIT_SERVER_GRACE_S); this picks up the rest so a user
    # with no PC setup still gets a video.
    _autoedit_dispatcher_task = _asyncio.create_task(_autoedit_dispatcher())
    print(f"[App][AutoEdit/server] auto-edit dispatcher started "
          f"(enabled={_autoedit_server_enabled()}, {AUTOEDIT_DISPATCH_INTERVAL_S}s tick, "
          f"{AUTOEDIT_SERVER_GRACE_S}s local-worker grace, "
          f"gate at {AUTOEDIT_MIN_AVAIL_MB}MB avail)", flush=True)

    # v938.8 — sweep abandoned render scratch. The work dir now lives on the
    # PERSISTENT disk so a deploy can resume instead of redoing the whole
    # render — which also means nothing deletes it by itself any more. Each
    # one holds a ~150MB source video plus intermediates, and a failed run
    # keeps its dir on purpose for diagnosis, so without this the disk fills
    # and takes the platform down with it.
    await _asyncio.to_thread(_sweep_old_autoedit_work)

    # v872 — idle recycle backstop (see _idle_memory_watchdog).
    _mem_watchdog_task = _asyncio.create_task(_idle_memory_watchdog())
    print(f"[App][Mem/v872] idle memory watchdog started "
          f"(recycle when idle and rss >= {RECYCLE_IDLE_RSS_MB}MB, "
          f"min uptime {RECYCLE_MIN_UPTIME_S}s; export gate at "
          f"{EXPORT_MIN_AVAIL_MB}MB avail)", flush=True)

    # Image platform: background poller for .done.json files from image_worker.py
    _image_stop_event = _asyncio.Event()
    _image_watch_task = _asyncio.create_task(_image_watch_done_files_loop(_image_stop_event))
    print("[App] Image platform watch task started", flush=True)

    yield
    
    # Shutdown
    _purge_task.cancel()
    _export_sweeper_task.cancel()
    _export_dispatcher_task.cancel()
    _autoedit_dispatcher_task.cancel()
    _mem_watchdog_task.cancel()
    _image_stop_event.set()
    try:
        await _asyncio.wait_for(_image_watch_task, timeout=3.0)
    except Exception:
        _image_watch_task.cancel()

    # v850 — the deploy path. This container is about to die with ffmpeg
    # mid-run; NULL the heartbeat so the NEXT container reclaims the export
    # immediately instead of waiting out the stale window.
    try:
        await _asyncio.to_thread(_requeue_local_exports_on_shutdown)
    except Exception as _sq:
        print(f"[Export/v850] shutdown re-queue failed: {_sq}", flush=True)

    # v938 — same handover for a render this container was in the middle of.
    try:
        await _asyncio.to_thread(_requeue_local_autoedits_on_shutdown)
    except Exception as _aq:
        print(f"[AutoEdit/server] shutdown re-queue failed: {_aq}", flush=True)

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
        # Routine assets / config pings — zero debug value.
        "/sw.js",
        "/api/me",
        "/auth/me",
        "/api/posthog-config",
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
        r"|/api/user-worker/jobs/[^/]+$"        # Worker job-detail poll (GET)
        r"|/api/user-worker/clips/redo-pending" # Worker redo poll
        r"|/api/user-worker/clips/kling-pending$"# Kling drain poll (heavy)
        r"|/api/user-worker/clips/[0-9]+/approval-status$"  # Approval poll
        r"|/api/user-worker/frames/"            # Frame serving to worker
        r"|/api/user-worker/tokens$"            # Token list poll
        r"|/api/user-worker/status$"            # Worker status poll
        r"|/api/local-worker/jobs/pending"      # Local worker job poll
        r"|/api/local-worker/clips/redo-pending"# Local worker redo poll
        r"|/api/voice-clone-warmup"             # Warmup ping
        # Image platform polling — frontend + worker
        r"|/api/images/nodes$"                  # Sidebar node list poll
        r"|/api/images/nodes/active$"           # Active-nodes poll (heavy 304 flood)
        r"|/api/images/nodes/[0-9]+$"           # Single node detail poll
        r"|/api/user/settings$"                 # User settings poll
        r"|/api/user/keys$"                     # User keys poll
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
        # v822.5 TEMPORARY: token-gated diag endpoint. It reaches its handler,
        # which enforces DIAG_TOKEN itself (inert unless the env var is set).
        "/api/diag/local-match",
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
        
        # v886: personal API token (Authorization: Bearer <UserWorkerToken>).
        # The middleware only needs to let the request reach its endpoint —
        # get_current_user re-validates the token there. Cached like sessions
        # to avoid a DB hit per poll.
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            import time as _time
            token_value = auth_header[7:].strip()
            if not hasattr(AuthMiddleware, '_session_cache'):
                AuthMiddleware._session_cache = {}
            bearer_cache_key = "bearer:" + token_value
            bearer_cached = AuthMiddleware._session_cache.get(bearer_cache_key)
            if bearer_cached and (_time.time() - bearer_cached['ts']) < 60:
                if bearer_cached['valid']:
                    return await call_next(request)
            else:
                from models import get_db, UserWorkerToken
                with get_db() as db:
                    tok = db.query(UserWorkerToken).filter(
                        UserWorkerToken.id == token_value,
                        UserWorkerToken.is_active == True
                    ).first()
                    valid = bool(tok and tok.user and tok.user.is_active)
                    AuthMiddleware._session_cache[bearer_cache_key] = {'valid': valid, 'ts': _time.time()}
                    # bearer-only traffic never reaches the cookie path's cap-eviction
                    # below, so cap here too (same 500-entry policy)
                    if len(AuthMiddleware._session_cache) > 500:
                        _now = _time.time()
                        AuthMiddleware._session_cache = {
                            k: v for k, v in AuthMiddleware._session_cache.items()
                            if _now - v['ts'] < 60
                        }
                    if valid:
                        # TEMP DIAG v886 — remove after operator-side evidence lands
                        print(f"[AuthMiddleware] v886 bearer accepted: {token_value[:8]}...", flush=True)
                        return await call_next(request)
                    print(f"[AuthMiddleware] v886 bearer REJECTED: {token_value[:8]}...", flush=True)
            # fall through: invalid bearer → 401 below

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


@app.get("/api/clip-duration-buckets")
def get_clip_duration_buckets():
    """v861 + v884 — serve the duration bucket tables to the frontend.

    The new-job dialogue validator has to show each line's render length while
    the operator types, which is too hot for a per-keystroke round trip. It
    fetches this once at page load and picks locally.

    Why an endpoint instead of just writing the table into index.html: the
    table has ONE home (clip_duration.CLIP_DURATION_BUCKETS). A hardcoded copy in the page
    would be a third site — after the module and the build auditor — free to
    drift out of step silently. Serving it keeps the JS a renderer of server
    data rather than a second implementation.

    v884 adds `char_buckets`; the frontend takes the longer of the two picks.
    An older cached page ignores the new key and keeps the pure-word answer —
    which is only ever the SHORTER one, so it under-reports rather than lies
    about a length the backend will not render.
    """
    return {
        "buckets": [list(b) for b in CLIP_DURATION_BUCKETS],   # [[max_words, seconds], ...]
        "char_buckets": [list(b) for b in CLIP_CHAR_BUCKETS],  # [[max_chars, seconds], ...] (v884)
        "allowed": list(ALLOWED_CLIP_DURATIONS_S),     # 4/6/8/10 — Flow can do all
        "veo_api": list(VEO_API_DURATIONS_S),          # 4/6/8 — the API folds 10→8
        "word_cap": CLIP_DURATION_BUCKETS[-1][0],              # v831 cap, amended to 28
        "char_cap": CLIP_CHAR_BUCKETS[-1][0],                  # v884 companion cap
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


@app.get("/api/user/settings")
async def get_user_settings(current_user: User = Depends(get_current_user)):
    """v815 — return the account's auto-image-retry mode (defaults to 'batch')."""
    return {"auto_image_retry": {"mode": parse_auto_image_retry_mode(current_user.settings_json)}}


class UserSettingsRequest(BaseModel):
    auto_image_retry_mode: str  # off | next | prev | batch


@app.put("/api/user/settings")
async def put_user_settings(
    req: UserSettingsRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v815 — update the account's auto-image-retry mode."""
    if req.auto_image_retry_mode not in VALID_RETRY_MODES:
        raise HTTPException(status_code=400, detail=f"invalid mode {req.auto_image_retry_mode!r}")
    try:
        data = json.loads(current_user.settings_json) if current_user.settings_json else {}
    except Exception:
        data = {}
    data.setdefault("auto_image_retry", {})["mode"] = req.auto_image_retry_mode
    current_user.settings_json = json.dumps(data)
    db.commit()
    return {"ok": True, "auto_image_retry": {"mode": req.auto_image_retry_mode}}


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
    
    # v861 (2026-07-18) — no duration coupling left. Resolution is no longer
    # selectable (Flow always exports 720p), so the 1080p→8s rule was dead, and
    # interpolation renders a 2-frame morph at any bucket, so its 8s rule fought
    # per-clip durations. Both removed. 4/6/8/10 are all valid for any job.

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

    # v943 owner scoping — POST /api/jobs takes swap_source_r2_key straight
    # from the request body, so it is caller input, not a fact. The image
    # import already refuses a key outside the caller's own prefix
    # (image_platform.py `_v943_own_prefix`); this route had no such guard, so
    # any authenticated user could bind another account's stored source into a
    # job and have the export read it. Same rule, same wording family, checked
    # BEFORE the Job row is written so nothing persists on refusal.
    _v943_own_prefix = f"swap-sources/{current_user.id}/"
    for _i, _line in enumerate(dialogue_list):
        if not isinstance(_line, dict):
            continue
        _key = _line.get('swap_source_r2_key')
        if _key is None:
            continue
        _key = str(_key)
        if not _key.startswith(_v943_own_prefix) or ".." in _key:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Clip {_i}: swap_source_r2_key is not one of yours. A "
                    f"charswap source must be uploaded through POST "
                    f"/api/images/swap-sources, which stores it under "
                    f"{_v943_own_prefix!r} (v943 owner scoping)."
                ),
            )


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
    
    # v861 — read once, outside the loop. Defaults to True so an older client
    # that never sends the field keeps the adaptive behavior.
    _adaptive_duration = bool(config_dict.get('adaptive_duration', True))
    if not _adaptive_duration:
        print(f"[v861/create] job {job_id}: adaptive length OFF — every clip "
              f"renders at the job duration ({config_dict.get('duration', '8')}s); "
              f"per-clip picks discarded", flush=True)

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
        # veo_render_duration_s is the per-clip render length (v861; was the
        # v667 ceil_to(target, [4,6,8]) bucket before that).
        cut_mode = line.get('cut_mode') if isinstance(line, dict) else None
        target_duration_s = line.get('target_duration_s') if isinstance(line, dict) else None
        veo_render_duration_s = line.get('veo_render_duration_s') if isinstance(line, dict) else None
        # v861 — adaptive length OFF: every clip renders at the job's single
        # `duration` setting. Storing NULL is the whole implementation: NULL
        # already means "use the job-level duration" on both render paths
        # (worker.veo_override_duration returns None; flow_worker falls back to
        # job.duration), so no render code needs to know this flag exists.
        # This deliberately discards any explicit `- **clip_duration_s:**` the
        # markdown declared — the operator asked for ONE duration for ALL clips.
        if not _adaptive_duration:
            veo_render_duration_s = None
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
            # v892 — background layer of an assembled frame. Set only when the
            # Scene declared `composite_plate_image:`; the Phase 3a sibling
            # loop renders it as a 'composite_plate' clip.
            composite_plate_image_node_id=(
                line.get('composite_plate_image_node_id')
                if isinstance(line, dict) else None
            ),
            # v718i (NEW 2026-05-18) — explicit end-frame image binding for
            # Veo native end-frame interpolation. NULL = sequential auto-inference.
            end_frame_image_node_id=end_frame_image_node_id_val,
            # v943 — charswap binding, carried from the ImageSceneAssignment
            # through prepare_batch_for_video's per-line metadata. All four
            # stay NULL on a clip that renders the normal way, and the worker
            # branches on render_method alone.
            render_method=(
                line.get('render_method') if isinstance(line, dict) else None
            ),
            swap_source_r2_key=(
                line.get('swap_source_r2_key') if isinstance(line, dict) else None
            ),
            swap_mode=(
                line.get('swap_mode') if isinstance(line, dict) else None
            ),
            swap_avatar_upload_id=(
                line.get('swap_avatar_upload_id') if isinstance(line, dict) else None
            ),
            # v943.1 — export-time source audio. NULL on every normal clip;
            # only the final export reads it.
            swap_audio=(
                line.get('swap_audio') if isinstance(line, dict) else None
            ),
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
                # v944 — the build's declared finishing (captions + overlay)
                # crosses here, read off the OWNED batch row the server already
                # loaded. Deliberately NOT a new field in the promote payload:
                # every hand-enumerated payload list in this platform has
                # drifted at least once (v892.2 / v892.5 / v892.8), and the
                # batch id the browser already sends is enough to fetch it.
                job.finishing_spec = getattr(batch, "finishing_spec", None)
                db.commit()
                print(f"[main.py] Stamped batch {request.image_batch_id} with promoted_video_job_id={job_id[:8]} "
                      f"finishing_spec={'set' if job.finishing_spec else 'none'}", flush=True)
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
            # Kling variant is FIRST CLIP ONLY (clip_index == 0). Audio-twin
            # pairs carry clip_index offset +100000, so this cleanly targets
            # only the first visual clip.
            db.execute(
                _sa_update(Clip)
                .where(Clip.job_id == job_id, Clip.clip_index == 0)
                .values(kling_variant_status='queued')
            )
            db.commit()
            add_job_log(db, job_id, "🎬 Kling variant queued for the first clip only — local worker will generate (Kling 3.0 + audio)", "INFO", "kling")
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

            # v871 — render-selectable prompt variant. Resolved ONCE per job
            # (not per clip). The batch that promoted this job is found via
            # ImageJobBatch.promoted_video_job_id (stamped synchronously in
            # the request handler, before this background task was spawned —
            # see the `if request.image_batch_id:` block above in the create
            # endpoint). Default 'omni' (no linked batch, or
            # batch.prompt_variant unset/'omni') leaves _v871_anchor_map EMPTY,
            # so the per-clip swap below is a strict no-op and
            # _veo_prompt_override / _veo_prompt_b resolve exactly as they did
            # pre-v871 — this is the regression-critical default path.
            from image_platform import ImageJobBatch, _parse_anchor_reference_prompts
            _v871_batch = db.query(ImageJobBatch).filter(
                ImageJobBatch.promoted_video_job_id == job_id
            ).first()
            _v871_variant = (getattr(_v871_batch, "prompt_variant", None) or "omni") if _v871_batch else "omni"
            _v871_anchor_map: Dict[Any, Any] = (
                _parse_anchor_reference_prompts(_v871_batch.source_markdown or "")
                if _v871_batch is not None and _v871_variant == "anchor"
                else {}
            )
            # Running per-scene counter — the anchor/omni sections both key
            # their `Clip N.M` labels as (scene_index, 1-based position of
            # this line within scene N), matching attach_veo_prompts_to_scenes
            # in veo_prompt_overrides.py. dialogue_raw preserves scene order,
            # so a running counter reproduces that same M ordinal here.
            _v871_scene_line_counter: Dict[int, int] = {}
            print(
                f"[v871] job {job_id[:8]} render variant={_v871_variant} "
                f"anchor_prompts={len(_v871_anchor_map)}",
                flush=True,
            )

            for idx in range(total_clips):
                line_data = dialogue_raw[idx] if isinstance(dialogue_raw[idx], dict) else {"text": dialogue_raw[idx]}
                dialogue_text = line_data.get("text", "")
                # v871 — track this line's (scene_index, ordinal-within-scene)
                # for the anchor-map lookup below. Cheap; runs regardless of
                # variant so numbering stays correct if the operator flips
                # the toggle on a redo.
                _v871_scene_idx = line_data.get("scene_index") if isinstance(line_data, dict) else None
                if _v871_scene_idx is not None:
                    _v871_scene_line_counter[_v871_scene_idx] = _v871_scene_line_counter.get(_v871_scene_idx, 0) + 1
                    _v871_line_ordinal = _v871_scene_line_counter[_v871_scene_idx]
                else:
                    _v871_line_ordinal = 1
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
                # v945.8 — a charswap scene must NEVER reach build_prompt with
                # no override. This is the same guard v943.5 put on the promote
                # path (image_platform.py ~12512), placed on the OTHER clip
                # creator, because the two are not symmetric and only one was
                # covered.
                #
                # Why the v943.5 shape does not protect this path: that guard
                # fires when prompt_text is EMPTY. Here, an empty override is
                # not empty downstream — build_prompt auto-constructs a full
                # DIALOGUE prompt from the line, writes it to prompt_text, and
                # the worker then trusts it because it is non-empty. That is
                # exactly the talking-head render this whole v943 chain exists
                # to stop, reachable through POST /api/jobs instead of promote.
                #
                # Scoped to charswap, so a job that never declares it is
                # bit-for-bit unaffected.
                if not _veo_prompt_override and (
                        (line_data.get("render_method") or "").strip().lower()
                        == "charswap"):
                    from image_platform import CHARSWAP_DEFAULT_PROMPT as _cs_default
                    _veo_prompt_override = _cs_default
                    print(f"[v945.8] clip {idx} is charswap but carried NO Veo "
                          f"prompt override — stamping the swap default so "
                          f"build_prompt cannot auto-construct a dialogue "
                          f"prompt for a silent swap scene", flush=True)
                _veo_negative_override = (line_data.get("veo_negative_prompt_override") or "").strip() or None
                # v805 — Prompt B policy fallback. Ships VERBATIM (no
                # build_prompt pass, no negative trailer): it is the
                # operator's authored voice-only fallback.
                _veo_prompt_b = (line_data.get("veo_prompt_b") or "").strip() or None
                # v821 — reworded dialogue line inside Prompt B (the spoken
                # line only). Carried onto clip.dialogue_text_b below.
                _veo_prompt_b_line = (line_data.get("veo_prompt_b_line") or "").strip() or None
                # v871 — render-variant swap. _v871_anchor_map is {} for every
                # 'omni' job (the default), so this whole block is skipped and
                # _veo_prompt_override / _veo_prompt_b keep the values resolved
                # above unchanged — that's the regression-critical guarantee.
                # Only when the linked batch declared prompt_variant='anchor'
                # do we look up this clip's (scene, ordinal) in the parsed
                # `## Anchor-Format Prompts` section and substitute its text
                # (Prompt A) / text_b (Prompt B) for the Omni-derived values.
                if _v871_variant == "anchor":
                    _v871_a = _v871_anchor_map.get((_v871_scene_idx, _v871_line_ordinal))
                    if _v871_a and _v871_a.get("text"):
                        _veo_prompt_override = _v871_a["text"]
                        _veo_prompt_b = _v871_a.get("text_b") or _veo_prompt_b
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
                                # v830 — cross-scene blend is EXPLICIT opt-in (was
                                # `!= "cut"`, which fired on None / "" / the literal
                                # "null" too — the parser stores `transition: null`
                                # as the string "null"). v782 set the default to cut
                                # but kept the != "cut" test; now only an explicit
                                # `transition: blend` interpolates. Twin of
                                # worker.py's storyboard-mode branch.
                                if next_scene.get("transition") == "blend":
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
                            # v827 — last_frame_index is the operator's explicit
                            # "End Frame" pick from the manual storyboard editor.
                            # The promote path no longer fabricates it, so this
                            # only fires when a human actually chose one. Bounds
                            # widened to reject negatives (a stored -1 used to
                            # index the LAST frame silently).
                            lfi = dialogue_data.get("last_frame_index")
                            if lfi is not None and isinstance(lfi, int) and 0 <= lfi < num_images:
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
                    # v805 — Prompt B verbatim (policy fallback; worker-side use).
                    clip.prompt_text_b = _veo_prompt_b
                    # v821 — reworded dialogue line inside Prompt B.
                    clip.dialogue_text_b = _veo_prompt_b_line
                    clip.start_frame = start_frame_key
                    clip.end_frame = end_frame_key
                    clip.status = ClipStatus.PENDING.value

            db.commit()
            print(f"[Background] ✓ All {total_clips} clip prompts committed", flush=True)
            add_job_log(db, job_id, f"✓ All {total_clips} prompts built", "INFO", "system")
            db.commit()

            # v892 Phase 3a-b — composite_plate sibling creation. Exactly the
            # shape of the audio_pair loop above, but pairing on the VIDEO axis:
            # a scene whose frame is assembled cannot be produced in one render,
            # so the performing clip ('composite_key') gets a sibling that
            # renders the background layer. The operator keys one over the
            # other; the plate is never concatenated (see the export filter).
            try:
                composite_key_clips = db.query(Clip).filter(
                    Clip.job_id == job_id,
                    Clip.clip_role == 'composite_key',
                ).all()
                if composite_key_clips:
                    composite_plate_offset = 200000
                    for ck in composite_key_clips:
                        if not ck.composite_plate_image_node_id:
                            print(
                                f"[v892/Phase3a] clip {ck.clip_index} is composite_key "
                                f"but carries no composite_plate_image_node_id — "
                                f"skipping plate creation",
                                flush=True,
                            )
                            continue
                        existing_plate = db.query(Clip).filter(
                            Clip.job_id == job_id,
                            Clip.clip_role == 'composite_plate',
                            Clip.paired_clip_id == ck.id,
                        ).first()
                        if existing_plate:
                            continue
                        cp = Clip(
                            job_id=job_id,
                            clip_index=composite_plate_offset + ck.clip_index,
                            dialogue_id=ck.dialogue_id,
                            # The plate is a silent background layer — no line.
                            dialogue_text='',
                            status='preparing',
                            scene_index=ck.scene_index,
                            clip_role='composite_plate',
                            paired_clip_id=ck.id,
                            # Render the plate from its own image, and hold the
                            # same duration as the layer it sits under.
                            composite_plate_image_node_id=ck.composite_plate_image_node_id,
                            target_duration_s=ck.target_duration_s,
                            veo_render_duration_s=ck.veo_render_duration_s,
                            cut_mode='auto',
                            scene_type='shot',
                        )
                        db.add(cp)
                        print(
                            f"[v892/Phase3a] composite_plate clip created for "
                            f"clip_index={ck.clip_index} scene={ck.scene_index} "
                            f"from image_node={ck.composite_plate_image_node_id}",
                            flush=True,
                        )
                    db.commit()

                    # v892.1 Phase 3b — give every plate clip a start frame and
                    # a prompt, then hand it to the worker. Phase 3a created the
                    # rows and stopped there: no prompt_text, no start_frame,
                    # status stuck at 'preparing', so the background layer of a
                    # composite open never rendered and the operator had nothing
                    # to key the performing layer over. Same shape as v698A
                    # Phase 3b below, minus the dialogue — a plate is silent by
                    # definition.
                    plate_clips = db.query(Clip).filter(
                        Clip.job_id == job_id,
                        Clip.clip_role == 'composite_plate',
                        Clip.status == 'preparing',
                    ).all()
                    plates_prepared = 0
                    for cp in plate_clips:
                        try:
                            ck_sib = db.query(Clip).filter(
                                Clip.id == cp.paired_clip_id
                            ).first()
                            if ck_sib is None or ck_sib.clip_index >= len(dialogue_raw):
                                print(
                                    f"[v892.1/Phase3b] plate {cp.id} has no usable "
                                    f"composite_key sibling; skipping",
                                    flush=True,
                                )
                                continue
                            line_data_cp = (
                                dialogue_raw[ck_sib.clip_index]
                                if isinstance(dialogue_raw[ck_sib.clip_index], dict)
                                else {}
                            )
                            plate_local_idx = line_data_cp.get(
                                "composite_plate_image_local_index"
                            )
                            if (
                                plate_local_idx is None
                                or plate_local_idx >= len(uploaded_frames_list)
                            ):
                                print(
                                    f"[v892.1/Phase3b] plate {cp.id} local index "
                                    f"{plate_local_idx} unusable "
                                    f"(uploaded_frames_list len="
                                    f"{len(uploaded_frames_list)}); skipping",
                                    flush=True,
                                )
                                continue
                            plate_fname = uploaded_frames_list[plate_local_idx]

                            # Operator-authored `### Clip S.L.plate` text wins.
                            # The fallback is deliberately literal rather than a
                            # build_prompt call: a plate is one still held for
                            # the layer's duration, and build_prompt would write
                            # motion and performance into a frame that must not
                            # move.
                            _authored_plate = (
                                (line_data_cp.get("composite_plate_prompt_override") or "").strip()
                                or None
                            )
                            _plate_secs = (
                                cp.veo_render_duration_s
                                or cp.target_duration_s
                                or 4
                            )
                            cp.prompt_text = _authored_plate or (
                                f"Animate the attached start-frame image into a "
                                f"{int(round(float(_plate_secs)))}-second vertical 9:16 video in "
                                f"which nothing moves. Hold the image completely "
                                f"still for the full duration, with no camera "
                                f"move, no zoom and no motion in the image. "
                                f"Silent. No subtitles, no captions."
                            )
                            cp.start_frame = f"jobs/{job_id}/frames/{plate_fname}"
                            cp.status = ClipStatus.PENDING.value
                            plates_prepared += 1
                            print(
                                f"[v892.1/Phase3b] plate {cp.id} → frame="
                                f"{plate_fname} prompt="
                                f"{'AUTHORED' if _authored_plate else 'fallback-hold'} "
                                f"status=pending",
                                flush=True,
                            )
                        except Exception as _cp_err:
                            print(
                                f"[v892.1/Phase3b] plate {cp.id} prep failed "
                                f"(non-fatal): {_cp_err}",
                                flush=True,
                            )
                    if plates_prepared:
                        db.commit()
                        print(
                            f"[v892.1/Phase3b] prepared {plates_prepared} "
                            f"composite_plate clip(s) → status=pending",
                            flush=True,
                        )
                    # TEMP DIAGNOSTIC (2026-08-17, remove once a composite open
                    # is confirmed rendered end to end) — proves the phase ran
                    # and says what it saw, so a silent zero is distinguishable
                    # from the phase never executing.
                    print(
                        f"[v892.1/Phase3b][TEMP] plate rows found="
                        f"{len(plate_clips)} prepared={plates_prepared} "
                        f"job={job_id}",
                        flush=True,
                    )
            except Exception as _exc_v892:
                # Never let plate creation take down the render flow; the key
                # layer still renders and the operator can composite manually.
                print(f"[v892/Phase3a] WARN composite plate creation failed: {_exc_v892}",
                      flush=True)

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

def active_dialogue_line(c):
    """v821 — the line that was actually SPOKEN: B's line if variant B rendered and a
    B line exists, else A's line. Used for export word-timing + captions."""
    if (c.get("rendered_prompt_variant") == "B") and (c.get("dialogue_text_b") or "").strip():
        return c["dialogue_text_b"]
    return c.get("dialogue_text")


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


def _clear_clip_qc(clip: Clip) -> None:
    """v939 — a QC report describes the exact takes that were on the clip when
    it was scored. Drop it the moment that stops being true (a redo lands, a
    variant is uploaded, versions are pruned, the render is replaced).

    A stale report is worse than none: it still renders in the review UI as a
    current verdict, and it poisons the agreement metric with disagreements
    that were never real. Mirrors image_platform._clear_qc.
    """
    clip.qc_json = None


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


def _job_finishing_spec(job):
    """The job's declared ## Finishing as a dict, or None. A corrupt stored
    value degrades to 'declared nothing' — same tolerance as queue_autoedit
    (it was validated at import; if it is broken now, the import is what to fix)."""
    raw = getattr(job, "finishing_spec", None)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        print(f"[Finishing/v947] job={job.id[:8]} finishing_spec is not valid JSON — ignored",
              flush=True)
        return None


def _export_defaults_payload(job):
    """v951 — what the Export dialog should open on for this job: the
    ExportSettings model defaults with the build's declared export_* folded on
    top, plus the names of the declared keys.

    Split out from the endpoint so it is unit-testable without a DB session —
    it touches only job.finishing_spec.
    """
    from auto_finish import export_modal_defaults
    return export_modal_defaults(_job_finishing_spec(job))


def _maybe_auto_finish_export(db, job):
    """v947 — when the job declares `auto_finish: on` and EVERY clip is
    approved, queue the export with the declared export_* settings. Called
    from approve_clip AND from update_job_finishing (v947.1); must never turn
    the caller's own work red (both callers catch). Returns (export_id,
    created) when the chain fired, None on every skip path.

    This is the FIRST half of the chain. The second half is
    _maybe_auto_finish_autoedit, fired by _export_runner when that export
    reaches DONE.

    The add_job_log below deliberately runs AFTER the export row is committed:
    losing the log line is better than losing the export. It can throw, and the
    caller's rollback+print is what absorbs that.
    """
    from auto_finish import auto_finish_on, all_clips_approved, derive_export_defaults
    spec = _job_finishing_spec(job)
    if not auto_finish_on(spec):
        return None
    # Row lock BEFORE the clip-status read, so the check-then-act below is
    # atomic against a second last-clip approval racing this one: the loser
    # blocks here, and by the time it reads, _queue_export_run finds the
    # winner's queued run and joins it instead of starting a second export.
    # Taken only after the cheap auto_finish_on gate — an ordinary approval on
    # a job that declared nothing never touches this lock. (sqlite ignores FOR
    # UPDATE; the tests still run, and --workers 1 makes it moot there anyway.)
    job = db.query(Job).filter(Job.id == job.id).with_for_update().first()
    if job is None:
        return None
    statuses = [s for (s,) in db.query(Clip.approval_status)
                .filter(Clip.job_id == job.id).all()]
    if not all_clips_approved(statuses):
        # Releases the FOR UPDATE lock; nothing to commit on this path.
        db.rollback()
        return None
    if not job.user_id:
        # A NULL user_id row could never be claimed/served downstream — same
        # invariant queue_autoedit enforces. Loud skip, not a crash.
        print(f"[AutoFinish] job={job.id[:8]} has no user_id — cannot auto-export",
              flush=True)
        # Releases the FOR UPDATE lock; nothing to commit on this path.
        db.rollback()
        return None
    settings = ExportSettings(**derive_export_defaults({}, spec, set()))
    run, created = _queue_export_run(db, job, settings, job.user_id)
    add_job_log(db, job.id,
                f"Auto-finish: all {len(statuses)} clips approved — export "
                f"{'queued' if created else 'already active, joined'} "
                f"({run.id[:8]})", "INFO", "auto_finish")
    print(f"[AutoFinish] job={job.id[:8]} all {len(statuses)} clips approved -> "
          f"export run={run.id[:8]} created={created}", flush=True)
    return run.id, created


@app.get("/api/jobs/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Get job details"""
    job = get_user_job(db, job_id, current_user)

    # v825.1 — self-heal completed_clips on read. completed_clips is a
    # denormalized cache; the DONE tri-counter binds to it (static/index.html
    # ~L10358). It drifts stale-low whenever a clip's status changes via a path
    # that forgets to bump it (pre-v825 manual variant upload, abandoned redo,
    # legacy flow-worker) — so a job with every clip rendered shows "10/11 done"
    # forever, and reloading never heals it because the poll faithfully returns
    # the stale stored value. Recompute live from the clip rows and persist so
    # the counter reflects reality. Single-job GET only — kept OUT of
    # _build_job_response so list_jobs does not incur an N+1 count query.
    _live_completed = db.query(Clip).filter(
        Clip.job_id == job_id,
        Clip.status == ClipStatus.COMPLETED.value,
    ).count()
    if _live_completed != (job.completed_clips or 0):
        _prev = job.completed_clips
        job.completed_clips = _live_completed
        _total = job.total_clips or 0
        if _total > 0:
            job.progress_percent = int((_live_completed / _total) * 100)
            if (
                _live_completed >= _total
                and job.status not in (
                    JobStatus.COMPLETED.value,
                    JobStatus.CANCELLED.value,
                    JobStatus.FAILED.value,
                )
            ):
                job.status = JobStatus.COMPLETED.value
                if job.completed_at is None:
                    job.completed_at = datetime.utcnow()
        db.commit()
        print(
            f"[v825.1 get_job] {job_id[:8]} completed_clips self-healed "
            f"{_prev} -> {_live_completed}/{_total}",
            flush=True,
        )

    resp = _build_job_response(job, approved_clips=_count_approved_clips(db, job_id))
    # v780 — surface the source image batch (if this video job was promoted from
    # an image job) so the UI can offer a "go to image job" button. Single-job
    # GET only — kept out of the shared serializer to avoid an N+1 in list_jobs.
    try:
        from image_platform import ImageJobBatch
        _b = (
            db.query(ImageJobBatch.id, ImageJobBatch.name)
            .filter(ImageJobBatch.promoted_video_job_id == job_id)
            .first()
        )
        if _b:
            resp.source_image_batch_id = _b[0]
            # v780.1 — also surface the batch's human name so the video-job
            # header can show the build title on top of the UUID.
            resp.source_image_batch_name = _b[1]
            print(f"[v780.1] job {job_id[:8]} promoted from image batch {str(_b[0])[:8]} name={_b[1]!r} (Image-job button + title shown)", flush=True)
    except Exception as _e:
        print(f"[v780] source_image_batch_id lookup skipped (non-fatal): {_e}", flush=True)
    return resp


@app.get("/api/jobs/{job_id}/export-defaults")
async def export_defaults(
    job_id: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v951 — the settings the Export dialog opens on for this job.

    Read-only. The dialog used to seed itself from localStorage (whatever this
    browser picked last, on any video); this gives it the VIDEO's own answer,
    declared in the build's `## Finishing` as export_* bullets. A job that
    declared nothing returns plain model defaults, which is what the dialog
    already did — no behaviour change for a build without the section.

    This does NOT change what the export runs: the modal still posts the
    controls the operator sees, so a manual change still wins. It changes what
    those controls START at.
    """
    job = get_user_job(db, job_id, current_user)  # 404/403 if not the caller's
    return _export_defaults_payload(job)


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
    # Either a raw HikerAPI key, or copy_key_from = id of an already-connected
    # account whose ENCRYPTED key gets reused as-is. The key never travels back
    # to a client; reusing it server-side is what lets a new handle be added
    # from a machine that does not hold the secret.
    api_key: str = ""
    copy_key_from: Optional[int] = None


class MatchInstagramVideoRequest(BaseModel):
    job_id: str
    # v953 — who is making this link. Defaults to 'manual' so the popover and
    # every existing caller are unchanged. An unattended caller (the reconciler)
    # sends 'ledger', which enforce_exclusivity treats as evictable: a machine
    # guess must stay correctable by the media-evidence matcher, and stamping it
    # 'manual' made every wrong reconciler link permanent.
    source: str = "manual"


# Copy view/like/comment counts onto an InstagramVideo row. Canonical home is
# instagram_autosync, which owns the sync body — /refresh-stats below applies the
# same rule, and two copies of it would drift.
from instagram_autosync import apply_counts as _ig_apply_counts  # noqa: E402


def _get_user_ig_account(db: DBSession, account_id: int, user: User):
    from models import InstagramAccount
    acc = db.query(InstagramAccount).filter_by(id=account_id).first()
    if not acc:
        raise HTTPException(status_code=404, detail="Instagram account not found")
    if acc.user_id != user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    return acc


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
    if req.copy_key_from is not None:
        src = _get_user_ig_account(db, req.copy_key_from, current_user)
        key_encrypted = src.api_key_encrypted
        print(f"[ig-accounts] @{handle}: reusing encrypted HikerAPI key of "
              f"account {src.id} @{src.handle}", flush=True)  # TEMP diagnostic
    elif (req.api_key or "").strip():
        key_encrypted = _enc_encrypt(req.api_key.strip())
    else:
        raise HTTPException(400, detail="Give api_key or copy_key_from")
    acc = InstagramAccount(
        user_id=current_user.id,
        handle=handle,
        api_key_encrypted=key_encrypted,
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
def sync_instagram_account(
    account_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    # 2026-08-29 — the body of this sync now lives in instagram_autosync, so the
    # manual button and the worker's unattended pass run THE SAME code. Two
    # copies would drift, and that drift shows up as "it works when I click it".
    from instagram_autosync import sync_account_once
    acc = _get_user_ig_account(db, account_id, current_user)
    res = sync_account_once(acc, db)
    if not res["ok"]:
        # sync_account_once swallows the exception on purpose (its other caller
        # is a worker loop that must not die), so the HTTP surface is unchanged:
        # an upstream failure is still a 502 here.
        raise HTTPException(status_code=502, detail=res["error"] or "sync failed")
    return {"added": res["added"], "total": res["total"],
            "thumbs_cached": res["thumbs_cached"]}


@app.get("/api/instagram/videos/{video_id}/thumb")
async def get_instagram_thumb(
    video_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Redirect to a presigned R2 url for a cached IG thumbnail.

    The bytes were downloaded + stored at sync time, so unlike the raw IG CDN
    url this never expires. The 302 itself is no-store (a presign is short-lived
    and must not be cached past expiry); R2/browser cache the bytes via the
    presigned response's own headers.
    """
    from models import InstagramVideo, InstagramAccount
    v = (
        db.query(InstagramVideo)
        .join(InstagramAccount, InstagramVideo.account_id == InstagramAccount.id)
        .filter(InstagramVideo.id == video_id, InstagramAccount.user_id == current_user.id)
        .first()
    )
    if not v or not v.thumb_r2_key:
        raise HTTPException(status_code=404, detail="No cached thumbnail")
    from backends.storage import is_storage_configured, get_storage
    if not is_storage_configured():
        raise HTTPException(status_code=404, detail="Storage not configured")
    try:
        storage = get_storage()
        presigned = storage.get_presigned_url(v.thumb_r2_key, expires_in=86400)
    except HTTPException:
        raise
    except Exception as e:
        # Storage-layer failure (bad creds / boto error) → clean 404, not a 500;
        # the frontend already has an onerror placeholder for a missing thumb.
        # Mirrors the try/except degrade in download_output.
        print(f"[IG thumb] presign failed video={video_id} key={v.thumb_r2_key}: {e}", flush=True)
        raise HTTPException(status_code=404, detail="Thumbnail unavailable")
    return RedirectResponse(url=presigned, status_code=302, headers={"Cache-Control": "no-store"})


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
    # Newest post first, exactly like the IG profile grid. id desc is the
    # tiebreaker so rows with no posted_at still land in a stable order.
    videos = q.order_by(
        InstagramVideo.posted_at.desc().nullslast(),
        InstagramVideo.id.desc(),
    ).all()
    return [v.to_dict() for v in videos]


@app.post("/api/instagram/accounts/{account_id}/refresh-stats")
def refresh_instagram_stats(
    account_id: int,
    days: int = 7,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Re-pull view/like/comment counts for reels posted in the last `days`.

    Cheap counterpart to /sync: sync walks every page of the account's history
    (up to 50 HikerAPI calls) to discover NEW reels. This one only wants FRESH
    NUMBERS on reels already stored, so it stops as soon as it has covered the
    window — 1-3 calls. No new rows are created here.
    """
    from models import InstagramVideo
    acc = _get_user_ig_account(db, account_id, current_user)
    api_key = _enc_decrypt(acc.api_key_encrypted)
    cutoff = datetime.utcnow() - timedelta(days=max(1, days))

    recent = (
        db.query(InstagramVideo)
        .filter(
            InstagramVideo.account_id == acc.id,
            InstagramVideo.posted_at.isnot(None),
            InstagramVideo.posted_at >= cutoff,
        )
        .all()
    )
    if not recent:
        return {"updated": 0, "checked": 0, "window_days": days, "detail": "no reels in window"}

    by_shortcode = {v.shortcode: v for v in recent}
    # Fetch past the window so a burst-posting day can't truncate it. The page
    # cap has to leave room for every in-window reel: a page is ~12 items, so
    # size it off the window itself instead of a flat guess.
    fetch_limit = len(recent) + 12
    max_pages = max(3, -(-fetch_limit // 10) + 1)  # ceil(limit/10) + 1 page of slack
    try:
        if not acc.ig_user_id:
            acc.ig_user_id = _ig_resolve_user_id(acc.handle, api_key)
        clips = _ig_fetch_recent_clips(acc.ig_user_id, api_key, limit=fetch_limit, max_pages=max_pages)
    except HikerAPIError as he:
        raise HTTPException(status_code=502, detail=str(he))

    updated = 0
    for c in clips:
        sc = c.get("shortcode")
        if not sc:
            print("[ig-stats] clip returned with no shortcode — skipped", flush=True)
            continue
        v = by_shortcode.pop(sc, None)
        if not v:
            continue  # clip outside the window, or not stored yet — /sync's job
        _ig_apply_counts(v, c)
        if c.get("thumb_url"):
            v.thumb_url = c["thumb_url"]
        updated += 1
    db.commit()
    # Anything still in by_shortcode is an in-window reel the fetch never
    # reached — surface it rather than reporting a clean run.
    missed = sorted(by_shortcode.keys())
    if missed:
        print(f"[ig-stats] account={acc.id} MISSED {len(missed)} in-window reels: {missed[:10]}", flush=True)
    print(f"[ig-stats] account={acc.id} window={days}d checked={len(recent)} updated={updated} missed={len(missed)}", flush=True)
    return {
        "updated": updated,
        "checked": len(recent),
        "missed": len(missed),
        "window_days": days,
    }


# === v878 — CSV export of posted-reel stats (2026-07-30) ===
# The panel shows views per card but there is no way to get the numbers OUT.
# Operator wants three columns over a chosen window: what the video is (name /
# URL / id), when it posted, how many views. CSV on purpose — opens in Sheets
# and Excel with no extra tooling.
def _ig_export_window(range_: str, month: Optional[str] = None):
    """(start, end, label) in UTC. `end` is EXCLUSIVE.

    A calendar month is NOT "30 days ago": asking for 2026-06 must return June
    only, so the month branch snaps to the 1st and rolls the year over.
    """
    now = datetime.utcnow()
    if range_ == "last_week":
        return now - timedelta(days=7), now, "last-7d"
    if range_ == "last_month":
        return now - timedelta(days=30), now, "last-30d"
    if range_ == "month":
        if not month:
            raise HTTPException(status_code=400, detail="range=month needs month=YYYY-MM")
        try:
            y_s, m_s = str(month).split("-")[:2]
            start = datetime(int(y_s), int(m_s), 1)
        except Exception:
            raise HTTPException(status_code=400, detail=f"bad month '{month}' — want YYYY-MM")
        end = datetime(start.year + 1, 1, 1) if start.month == 12 else datetime(start.year, start.month + 1, 1)
        return start, end, start.strftime("%Y-%m")
    raise HTTPException(status_code=400, detail=f"bad range '{range_}' — want last_week|last_month|month")


def _ig_export_rows(db, account_id: int, start, end):
    """Reels of one account posted in [start, end), newest first, as CSV rows.

    video_title (v878.1) = the BUILD title of the matched job: the name of the
    image batch this video job was promoted from, which is what v780.1 already
    shows in the video-job header. That is the only human-authored title in the
    system — export_basename is a minted machine name (final_export_<id>_<ts>)
    and no Job column holds a title. Empty when the reel has no match, or when
    the job was not promoted from a batch.

    video_name resolution, in order: the matched job's FIRST clip line (that is
    what the Jobs board titles a job with, see _build_job_response
    first_dialogue) → first line of the caption → the shortcode. A reel with no
    posted_at cannot be placed in a window, so it is dropped and counted.
    """
    from models import InstagramVideo, Clip
    videos = (
        db.query(InstagramVideo)
        .filter(
            InstagramVideo.account_id == account_id,
            InstagramVideo.posted_at.isnot(None),
            InstagramVideo.posted_at >= start,
            InstagramVideo.posted_at < end,
        )
        .order_by(InstagramVideo.posted_at.desc(), InstagramVideo.id.desc())
        .all()
    )
    job_ids = [v.matched_job_id for v in videos if v.matched_job_id]
    job_name = {}
    if job_ids:
        clips = (
            db.query(Clip.job_id, Clip.clip_index, Clip.dialogue_text)
            .filter(Clip.job_id.in_(job_ids))
            .order_by(Clip.job_id, Clip.clip_index)
            .all()
        )
        for c in clips:
            # setdefault → the LOWEST clip_index wins (rows come in asc order).
            job_name.setdefault(c.job_id, (c.dialogue_text or "").strip()[:80])

    # v878.1 — build title per matched job. Same lookup as v780.1 in get_job,
    # batched here so a 30-day export is one query, not one per reel. Wrapped:
    # a missing image_job_batches table (old DB / partial migration) must cost
    # the operator a blank column, not the whole export.
    job_title = {}
    if job_ids:
        try:
            from image_platform import ImageJobBatch
            for jid, bname in (
                db.query(ImageJobBatch.promoted_video_job_id, ImageJobBatch.name)
                .filter(ImageJobBatch.promoted_video_job_id.in_(job_ids))
                .all()
            ):
                if jid and bname:
                    job_title.setdefault(jid, str(bname).strip()[:200])
        except Exception as e:
            print(f"[ig-export] build-title lookup skipped (non-fatal): {e}", flush=True)

    rows = []
    for v in videos:
        name = job_name.get(v.matched_job_id) or ""
        if not name and v.caption:
            name = v.caption.strip().splitlines()[0][:80] if v.caption.strip() else ""
        rows.append([
            job_title.get(v.matched_job_id, ""),
            name or v.shortcode or "",
            v.url or "",
            v.id,
            v.posted_at.strftime("%Y-%m-%d %H:%M") if v.posted_at else "",
            v.views or 0,
        ])
    undated = (
        db.query(InstagramVideo)
        .filter(InstagramVideo.account_id == account_id, InstagramVideo.posted_at.is_(None))
        .count()
    )
    return rows, undated


@app.get("/api/instagram/accounts/{account_id}/export")
def export_instagram_videos(
    account_id: int,
    range: str = "last_week",
    month: Optional[str] = None,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Download build title / name / URL / id / posting date / views as CSV."""
    import csv as _csv
    import io as _io

    acc = _get_user_ig_account(db, account_id, current_user)
    start, end, label = _ig_export_window(range, month)
    rows, undated = _ig_export_rows(db, acc.id, start, end)

    buf = _io.StringIO()
    w = _csv.writer(buf, lineterminator="\n")
    w.writerow(["video_title", "video_name", "video_url", "video_id", "posted_at", "views"])
    w.writerows(rows)
    print(
        f"[ig-export] account={acc.id} @{acc.handle} range={range} month={month} "
        f"window={start.date()}..{end.date()} rows={len(rows)} undated_skipped={undated}",
        flush=True,
    )
    fname = f"ig-{acc.handle}-{label}.csv"
    # utf-8-sig: Excel on Windows reads a BOM-less utf-8 csv as latin-1 and
    # mangles any accented caption text.
    return Response(
        content=buf.getvalue().encode("utf-8-sig"),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{fname}"',
            "X-IG-Export-Rows": str(len(rows)),
            "X-IG-Export-Undated-Skipped": str(undated),
        },
    )


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
def suggest_matches(   # sync ON PURPOSE — see below
    video_id: int,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    # NOT `async def`: this handler probes exports from R2 (download + ffmpeg,
    # export_probe.evidence_candidates) and runs blocking sync SQLAlchemy. Inside
    # an `async def` every one of those blocks the SINGLE event loop of the one
    # uvicorn worker — every other request in the process stalls behind it, and a
    # block past gunicorn's --timeout 300 gets the worker SIGABRT'd, killing the
    # in-flight DB connections (the 2026-07-06 outage). Declared plain `def`,
    # FastAPI runs it in the anyio threadpool instead, off the loop. There is no
    # `await` in this body — keep it that way, or move the blocking work into
    # asyncio.to_thread first.
    from models import InstagramVideo, InstagramAccount, Job
    from sqlalchemy import or_
    v = db.query(InstagramVideo).filter_by(id=video_id).first()
    if not v:
        raise HTTPException(404, detail="video not found")
    acc = db.query(InstagramAccount).filter_by(id=v.account_id).first()
    if not acc or acc.user_id != current_user.id:
        raise HTTPException(403, detail="access denied")
    if not v.transcription or v.transcription_status != "done":
        # v852 — same object shape as the happy path, with a verdict the UI can
        # tell apart. The old bare `[]` was indistinguishable from an empty pool,
        # so the popover blamed the candidate pool ("no jobs to match against")
        # for what is really a reel that has not finished transcribing — sending
        # the operator hunting for a problem that does not exist.
        return {
            "verdict": "not_transcribed",
            "top": 0.0,
            "gap": 0.0,
            "suggestions": [],
            "transcription_status": v.transcription_status,
        }
    # Candidate pool = jobs that actually REACHED the finishing lane. A reel on
    # IG was necessarily exported first, so a job that never exported cannot be
    # its source — matching one wrongly stamps `published` onto a job still
    # sitting in approval/export.
    #
    # Gate on has_export, NOT on the stored lifecycle_stage column: the stored
    # column is persisted lazily and goes stale (b-roll/twin jobs sit at
    # awaiting_export long after exporting), which is why the stage filter was
    # dropped here in the first place. has_export is the same underlying state
    # derive_effective_stage() reads to return AWAITING_FINISHING, so it's the
    # durable signal — it can't go stale the way the derived column does.
    # lifecycle_stage IN (finishing, published) is OR'd in to honour an operator
    # who manually parked a job in the lane, and to keep already-published jobs
    # eligible for instagram_url back-fill (drive/local watch publish with
    # instagram_video_id still NULL).
    #
    # v857 — a job already linked to ANOTHER reel stays in the pool (the
    # `instagram_video_id IS NULL` filter is gone). Hiding it did not protect
    # anything: it only meant that once a reel took a job — rightly or wrongly —
    # the popover could never offer that job to the reel that actually owns it,
    # so a wrong link was unfixable from the UI. It is shown, flagged
    # `already_linked_to`, and the operator decides. A warning, not a block.
    candidates = (
        db.query(Job)
        .filter(
            Job.user_id == current_user.id,
            Job.status == "completed",
            Job.archived == False,  # noqa: E712
            or_(
                Job.has_export == True,  # noqa: E712
                Job.lifecycle_stage.in_(["awaiting_finishing", "published"]),
            ),
        )
        .all()
    )
    # v852 — a job created AFTER the reel was posted cannot be its source. A hard
    # fact, and it separates near-duplicate twins (same shared script, built days
    # apart) that the WORDS alone cannot tell apart. Applied in Python, not SQL,
    # so an absent posted_at can never silently empty the pool.
    #
    # ONLY this half of the window applies here. v855's full
    # within_recency_window ALSO drops jobs built >30d BEFORE the post — right for
    # the AUTO-PUBLISH matchers (instagram/local/drive_transcribe), wrong here.
    # This endpoint feeds the MANUAL suggestions popover, and the entire design is
    # "no evidence -> a human picks". Pruning an old-but-real job means the human
    # cannot pick it — the tool silently hides the correct answer, and the
    # "measured max job age: 20.99 days" that set the 30d bound is an observation,
    # not a law. Being late is not proof; being IMPOSSIBLE (built after the post)
    # is, so only that filter stays.
    _before = len(candidates)
    candidates = [
        j for j in candidates
        if _ig_match.job_predates_post(j.created_at, v.posted_at)
    ]
    if _before != len(candidates):
        print(f"[ig-suggest] video={video_id} dropped {_before - len(candidates)} "
              f"job(s) created AFTER posted_at={v.posted_at}", flush=True)
    # v822.6: the manual suggestions now use the SAME content matcher as the
    # local auto-matcher — rare-term-weighted TF-IDF cosine (idf_power=2),
    # validated on the operator's real data. The old char-level `best_matches`
    # scored near-duplicate scripts at ~1.000 against the wrong twin, which is
    # exactly why suggestions "landed far off". One bulk dialogue query
    # (no per-candidate N+1), top-5 regardless of score so the UI can show
    # even low-confidence options.
    from local_transcribe import _bulk_dialogue_map, _MATCH_IDF_POWER, _MATCH_HIGH, _MATCH_MARGIN
    dmap = _bulk_dialogue_map(db, [j.id for j in candidates])
    pairs = [(j.id, dmap.get(j.id, "")) for j in candidates]
    full_ranked = _ig_match.rank_tfidf(v.transcription or "", pairs, idf_power=_MATCH_IDF_POWER)
    # v852 — verdict is judged on the FULL ranking, BEFORE the top-5 slice: the
    # runner-up that makes a match ambiguous still counts even when it is not
    # among the five rows we show.
    verdict = _ig_match.match_verdict(full_ranked, _MATCH_HIGH, _MATCH_MARGIN)
    ranked = full_ranked[:5]

    # v855 — the MEDIA gets the last word. The reel's runtime + loudness envelope
    # vs each export's: where the words tie (identical scripts scored EXACTLY
    # equal on the 14 disputed reels), the render does not. Only the text-ranked
    # shortlist is probed from R2 (see export_probe.LAZY_PROBE_CAP) — probing the
    # whole pool inside a web request is what times the worker out.
    evidence = None
    try:
        from export_probe import evidence_candidates
        _cands = evidence_candidates(
            db, candidates, priority_ids=[r["job_id"] for r in ranked],
        )
        ev = _ig_match.evidence_pick(v.duration_s, v.audio_fp, _cands)
        if ev["job_id"]:
            evidence = {
                "source": ev["source"],
                "similarity": ev["similarity"],
                "dur_delta": ev["dur_delta"],
            }
            # The proven job goes FIRST, even when the text put it nowhere — and
            # even when it never made the text top-5 at all.
            proven_id = ev["job_id"]
            proven_score = next(
                (r["score"] for r in full_ranked if r["job_id"] == proven_id), 0.0,
            )
            ranked = (
                [{"job_id": proven_id, "score": proven_score}]
                + [r for r in ranked if r["job_id"] != proven_id]
            )[:5]
            verdict = {"verdict": "proven", "top": verdict["top"], "gap": verdict["gap"]}
    except Exception as _e:   # evidence is a bonus; never break the popover
        print(f"[ig-suggest] video={video_id} evidence failed: "
              f"{type(_e).__name__}: {str(_e)[:160]}", flush=True)

    print(f"[ig-suggest] video={video_id} pool={len(candidates)} verdict={verdict['verdict']} "
          f"top={verdict['top']:.3f} gap={verdict['gap']:.3f} evidence={evidence} "
          f"top5={[(r['job_id'][:8], r['score']) for r in ranked]}", flush=True)

    # v857 — WARN when a suggested job is already another reel's. A job produced
    # ONE video, so linking it twice means one of the two links is false; the
    # popover has to say so instead of letting the operator make the same wrong
    # link by hand that the auto-matcher now refuses. A REPOST is the exception —
    # one export posted twice legitimately claims one job — so a reel that is the
    # SAME FILE as the holder is not flagged. Warning, never a block: the operator
    # can still pick the row, which is how a wrong link gets repaired.
    holders = {}
    _linked = (
        db.query(InstagramVideo)
        .filter(
            InstagramVideo.matched_job_id.in_([r["job_id"] for r in ranked]),
            InstagramVideo.id != v.id,
        )
        .all()
    ) if ranked else []
    for other in _linked:
        if other.matched_job_id in holders:
            continue
        if _ig_match.is_same_video(v.audio_fp, other.audio_fp, v.duration_s, other.duration_s):
            print(f"[ig-suggest] video={video_id} job={str(other.matched_job_id)[:8]} is held by "
                  f"{other.shortcode} but they are the SAME FILE (repost) — not flagged", flush=True)
            continue
        holders[other.matched_job_id] = other.shortcode

    top = []
    for r in ranked:
        clip = db.query(Clip).filter(Clip.job_id == r["job_id"], Clip.clip_index == 0).first()
        slug = (clip.dialogue_text or "")[:80] if clip and clip.dialogue_text else r["job_id"][:8]
        row = {"job_id": r["job_id"], "score": r["score"], "slug": slug}
        if r["job_id"] in holders:
            row["already_linked_to"] = holders[r["job_id"]]
        top.append(row)
    if holders:
        print(f"[ig-suggest] video={video_id} already-linked jobs in the list: "
              f"{[(k[:8], s) for k, s in holders.items()]}", flush=True)
    return {
        "verdict": verdict["verdict"],
        "top": verdict["top"],
        "gap": verdict["gap"],
        "evidence": evidence,
        "suggestions": top,
    }


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
    # A reel on IG was exported before it was posted, so a job that never
    # exported and was never parked in the finishing lane cannot be its source.
    # Not blocked (an operator override stays possible), but loud — this is the
    # signature of a match landing on a job from the wrong lane.
    if not job.has_export and job.lifecycle_stage not in ("awaiting_finishing", "published"):
        print(f"[ig-match] WARNING video={video_id} matched job={job.id[:8]} that never "
              f"reached finishing (has_export={job.has_export} stage={job.lifecycle_stage})",
              flush=True)
    # v857.1 — RELEASE THE PREVIOUS HOLDER. A job produced ONE video and nothing in
    # the schema enforces it, so writing this link without clearing the last one
    # leaves TWO reels pointing at one job — and the phantom holder then poisons
    # find_job_incumbent. The popover says "already linked to X — picking this
    # moves the link"; this is what makes that true. A repost (the same export
    # posted twice) is the one legitimate double claim and is left alone.
    from local_transcribe import release_other_holders
    released = release_other_holders(db, job, v, "instagram")
    if released:
        print(f"[ig-match] video={video_id} job={job.id[:8]} moved the link off "
              f"{', '.join(released)}", flush=True)
    v.matched_job_id = job.id
    v.matched_at = datetime.utcnow()
    # A HUMAN MADE THIS LINK. The unattended matcher must never evict it: a manual
    # pick carries no media evidence, so it would score ~0 against any waveform
    # challenger, and the displaced reel is never re-matched (transcribe_one is
    # idempotent on 'done') — the operator's repair would be destroyed, not moved.
    #
    # v953 — UNLESS the caller says it is not a human. The reconciler writes
    # through this same endpoint, and stamping its evidence-derived guess
    # 'manual' made every wrong link permanent by disabling the very matcher
    # that could disprove it. Only 'ledger' is accepted as an alternative;
    # anything else falls back to 'manual', so a typo can never weaken a link.
    v.match_source = "ledger" if (req.source or "").strip().lower() == "ledger" else "manual"
    if v.match_source != "manual":
        print(f"[ig-match] video={video_id} job={job.id[:8]} linked by "
              f"source={v.match_source} (evictable by media evidence)", flush=True)
    job.instagram_url = v.url
    job.instagram_video_id = v.id
    # Record WHO published the job, mirroring drive_watch / local_watch. Without
    # this, unmatch cannot tell a job THIS match dragged into `published` from
    # one that drive/local legitimately published and we merely back-filled a
    # url onto — so it left every wrongly-matched job stuck in the published
    # lane forever. Only claim provenance when we are the one publishing it.
    if job.lifecycle_stage != "published":
        job.lifecycle_stage = "published"
        job.published_via = "ig_match"
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
    from lifecycle import derive_effective_stage
    matched_job_id = v.matched_job_id
    v.matched_job_id = None
    v.matched_at = None
    v.match_source = None   # the link is gone; its provenance goes with it
    reverted_to = None
    if matched_job_id:
        job = db.query(Job).filter_by(id=matched_job_id).first()
        if job:
            job.instagram_url = None
            job.instagram_video_id = None
            # Undo the publish this match caused. Clearing the url alone left the
            # job parked in `published` — so a wrong match was unrecoverable and
            # an unfinished job sat at the end of the board.
            #
            # Only revert when THIS match did the publishing: published_via is
            # 'ig_match', or NULL for rows matched before provenance was
            # recorded (drive/local always stamp their own token, so NULL here
            # means the IG match published it). A drive/local-published job keeps
            # its published state — unlinking the reel doesn't unpublish it.
            if (job.published_via or "ig_match") == "ig_match":
                job.lifecycle_stage = None  # re-derive from live state, not the stale column
                job.lifecycle_stage = derive_effective_stage(
                    job, _count_approved_clips(db, job.id)
                )
                job.published_via = None
                job.published_at = None
                reverted_to = job.lifecycle_stage
                print(f"[ig-unmatch] video={video_id} job={job.id[:8]} "
                      f"reverted published -> {reverted_to}", flush=True)
    db.commit()
    return {"unmatched": video_id, "reverted_job_stage": reverted_to}


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
    # existing row + skip re-processing — UNLESS the row is failed/stuck
    # (v822): those were permanent misses, now they re-run the pipeline.
    existing = (
        db.query(LocalVideo)
        .filter_by(user_id=current_user.id, file_hash=file_hash)
        .first()
    )
    if existing:
        from local_transcribe import should_reprocess
        if should_reprocess(existing.transcription_status, existing.created_at):
            blob = await file.read()
            if not blob or len(blob) < 1024:
                raise HTTPException(400, detail=f"file too small ({len(blob)}B)")
            if len(blob) > 500 * 1024 * 1024:
                raise HTTPException(413, detail="file > 500MB")
            print(f"[local] v822 reprocess hash={file_hash[:8]} (was {existing.transcription_status})", flush=True)
            existing.file_name = file_name
            existing.size_bytes = len(blob)
            existing.transcription_status = "pending"
            existing.transcription_error = None
            existing.transcription = None
            db.commit()
            # to_thread: transcribe_local is ffmpeg + Whisper + an R2-probing
            # match — tens of seconds of BLOCKING work. This handler must stay
            # `async def` (it awaits file.read()), so the blocking call is pushed
            # off the event loop by hand, or it freezes every other request in
            # the worker.
            await asyncio.to_thread(transcribe_local, existing, blob, db)
            db.refresh(existing)
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

    # Transcribe within the request lifetime (Render has a 60s+ HTTP timeout;
    # ffmpeg + whisper on a 30s reel is ~10-20s) — but on a WORKER THREAD, not
    # the event loop. Called inline it blocked the loop for those 10-20s and
    # every other request in the process queued behind it.
    await asyncio.to_thread(transcribe_local, v, blob, db)
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


@app.post("/api/local-videos/rematch")
def rematch_local_videos(   # sync ON PURPOSE — see suggest_matches
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v822: sweep done-but-unmatched local videos against the current
    awaiting_finishing pool.  Browser calls this once per poll cycle.

    NOT `async def`. The browser fires this unattended on EVERY poll cycle and
    the sweep reaches R2 (rematch_unmatched -> _maybe_auto_match ->
    evidence_candidates: downloads + ffmpeg). That is the worst possible thing
    to run on the event loop — plain `def` puts it in the anyio threadpool.
    No `await` in this body; keep it that way.
    """
    from local_transcribe import rematch_unmatched
    return rematch_unmatched(current_user.id, db)


@app.get("/api/diag/local-match")
def diag_local_match(
    token: str = "",
    user_id: str = "",
    limit: int = 20,
    only_unmatched: int = 0,
    pool: str = "pending",
    request_compare: int = 0,
    db: DBSession = Depends(get_db_session),
):
    """v822.5 TEMPORARY read-only diagnostic (NO auth — gated by DIAG_TOKEN,
    inert unless that env var is set). For each local video, ranks its
    transcript against a candidate pool and returns the top-5 + the auto/manual
    decision. `pool=pending` (default) = the live awaiting_finishing pool (how
    the real matcher sees it). `pool=full` = ALL of the user's completed jobs
    (with dialogue) — this is how we RE-CHECK already-matched videos, because
    a video's linked job leaves the pending pool once matched. For a matched
    video it reports where its STORED job ranks in the full library
    (stored_rank / stored_score): rank 1 = link consistent, high rank = the
    link is probably WRONG. REMOVE after calibration (operator-authorized
    2026-07-07)."""
    import os as _os
    expected = _os.environ.get("DIAG_TOKEN", "")
    if not expected or token != expected:
        raise HTTPException(status_code=404, detail="not found")

    from models import LocalVideo, Job, Clip
    from local_transcribe import _bulk_dialogue_map, _MATCH_HIGH, _MATCH_MARGIN
    import instagram_match as _ig

    limit = max(1, min(int(limit or 20), 200))
    full_pool = (pool == "full")
    q = db.query(LocalVideo)
    if user_id:
        q = q.filter(LocalVideo.user_id == user_id)
    if only_unmatched:
        q = q.filter(LocalVideo.matched_job_id == None)  # noqa: E711
    vids = q.order_by(LocalVideo.created_at.desc()).limit(limit).all()

    def _first_line(job_id):
        c = (
            db.query(Clip.dialogue_text, Clip.voiceover_line)
            .filter(Clip.job_id == job_id)
            .order_by(Clip.clip_index.asc())
            .first()
        )
        if not c:
            return ""
        return ((c[1] or c[0]) or "")[:70]

    out = []
    _pool_cache = {}
    for v in vids:
        if v.user_id not in _pool_cache:
            jq = db.query(Job).filter(Job.user_id == v.user_id)
            if full_pool:
                jq = jq.filter(Job.status == "completed")
            else:
                jq = jq.filter(Job.lifecycle_stage == "awaiting_finishing")
            cand = jq.all()
            dmap = _bulk_dialogue_map(db, [j.id for j in cand])
            _pool_cache[v.user_id] = (cand, dmap)
        cand, dmap = _pool_cache[v.user_id]
        pairs = [(j.id, dmap.get(j.id, "")) for j in cand]
        ranked_full = _ig.rank_tfidf(v.transcription or "", pairs)
        ranked = ranked_full[:5]
        pick = _ig.auto_pick(ranked, _MATCH_HIGH, _MATCH_MARGIN) if ranked else None
        s1 = ranked[0]["score"] if ranked else 0.0
        s2 = ranked[1]["score"] if len(ranked) > 1 else 0.0

        def _stored_pos(rk):
            if not v.matched_job_id:
                return (None, None)
            for i, r in enumerate(rk):
                if r["job_id"] == v.matched_job_id:
                    return (i + 1, r["score"])
            return (None, None)

        # RE-CHECK a matched video: where does its STORED job rank in the pool?
        stored_rank, stored_score = _stored_pos(ranked_full)
        stored_line = _first_line(v.matched_job_id) if v.matched_job_id else None

        # v822.6 metric bake-off: compare tfidf(idf^1) vs tfidf(idf^2) vs bm25
        # on the SAME pool. Reports stored-job rank + top1/top2 per ranker so
        # we can pick the ranker that keeps correct matches #1 with the widest
        # margin AND suppresses the generic attractor.
        compare = None
        if int(request_compare):
            r2 = _ig.rank_tfidf(v.transcription or "", pairs, idf_power=2.0)
            rb = _ig.rank_bm25(v.transcription or "", pairs)
            def _pack(rk):
                sr, ss = _stored_pos(rk)
                return {
                    "top1": rk[0]["job_id"][:8] if rk else None,
                    "s1": rk[0]["score"] if rk else 0.0,
                    "s2": rk[1]["score"] if len(rk) > 1 else 0.0,
                    "stored_rank": sr, "stored_score": ss,
                }
            compare = {"tfidf1": _pack(ranked_full), "tfidf2": _pack(r2), "bm25": _pack(rb)}

        out.append({
            "compare": compare,
            "file_name": v.file_name,
            "hash": (v.file_hash or "")[:8],
            "status": v.transcription_status,
            "matched_job_id": (v.matched_job_id or "")[:8] or None,
            "match_score": v.match_score,
            "stored_rank": stored_rank,
            "stored_score": stored_score,
            "stored_line": stored_line,
            "transcript_len": len(v.transcription or ""),
            "transcript": (v.transcription or "")[:500],
            "pool": len(cand),
            "s1": s1, "s2": s2, "margin": round(s1 - s2, 4),
            "decision": ("AUTO->" + str(pick)[:8]) if pick else "MANUAL (ambiguous/low)",
            "top5": [
                {"job": r["job_id"][:8], "score": r["score"],
                 "line1": _first_line(r["job_id"]),
                 "dlg_head": dmap.get(r["job_id"], "")[:140]}
                for r in ranked
            ],
        })
    return {
        "count": len(out),
        "pool_mode": pool,
        "high": _MATCH_HIGH,
        "margin": _MATCH_MARGIN,
        "note": "v822.5 temporary diag; set DIAG_TOKEN in Render to enable, unset to disable",
        "videos": out,
    }


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
# v815 — prominent-people / celebrity codes that trigger image auto-retry.
PROMINENT_PEOPLE_ERROR_CODES = frozenset({
    "PROMINENT_PEOPLE_FILTER",
    "CELEBRITY_FILTER",
    "CELEBRITY_RAI_FILTER",
})

IMAGE_ATTRIBUTABLE_ERROR_CODES = frozenset({
    "CONTENT_POLICY_VIOLATION",
    "PROMINENT_PEOPLE_FILTER",  # v815 — manual replace card still works when auto-retry off/exhausted
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
            attempts_remaining=UNLIMITED_ATTEMPTS_REMAINING,
            redo_reason=c.redo_reason,
            versions=deduplicate_versions(c.versions_json, job_id=c.job_id),
            selected_variant=c.selected_variant if c.selected_variant else 1,
            total_variants=get_actual_versions_count(c),
            clip_mode=c.clip_mode or "fresh",
            scene_index=c.scene_index or 0,
            # v861.2 — enumerate these HERE too: ClipResponse is built with
            # explicit kwargs, so declaring a field on the model is not enough.
            target_duration_s=c.target_duration_s,
            veo_render_duration_s=c.veo_render_duration_s,
            prompt_text=c.prompt_text or None,
            prompt_text_b=c.prompt_text_b or None,  # v805/v821
            dialogue_text_b=c.dialogue_text_b or None,  # v821
            rendered_prompt_variant=c.rendered_prompt_variant or "A",  # v821
            in_lineup=c.id in lineup_set if lineup_set else True,
            # v698A
            clip_role=c.clip_role,
            paired_clip_id=c.paired_clip_id,
            voiceover_anchor_image_node_id=c.voiceover_anchor_image_node_id,
            voiceover_line=c.voiceover_line,
            # v718i (NEW 2026-05-18) — Veo native end-frame interpolation binding
            end_frame_image_node_id=c.end_frame_image_node_id,
            replacement_start_frame=c.replacement_start_frame,  # v701
            auto_image_retry=(
                json.loads(c.auto_image_retry_json) if c.auto_image_retry_json else None
            ),  # v815
            qc=c._safe_qc(),  # v939 shadow-mode clip QC
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
            attempts_remaining=UNLIMITED_ATTEMPTS_REMAINING,
            redo_reason=c.redo_reason,
            versions=deduplicate_versions(c.versions_json, job_id=c.job_id),
            selected_variant=c.selected_variant if c.selected_variant else 1,
            total_variants=get_actual_versions_count(c),
            clip_mode=c.clip_mode or "fresh",
            scene_index=c.scene_index or 0,
            # v861.2 — enumerate these HERE too: ClipResponse is built with
            # explicit kwargs, so declaring a field on the model is not enough.
            target_duration_s=c.target_duration_s,
            veo_render_duration_s=c.veo_render_duration_s,
            prompt_text=c.prompt_text or None,
            prompt_text_b=c.prompt_text_b or None,  # v805/v821
            dialogue_text_b=c.dialogue_text_b or None,  # v821
            rendered_prompt_variant=c.rendered_prompt_variant or "A",  # v821
            in_lineup=c.id in lineup_set if lineup_set else True,
            clip_role=c.clip_role,
            paired_clip_id=c.paired_clip_id,
            voiceover_anchor_image_node_id=c.voiceover_anchor_image_node_id,
            voiceover_line=c.voiceover_line,
            # v718i (NEW 2026-05-18) — Veo native end-frame interpolation binding
            end_frame_image_node_id=c.end_frame_image_node_id,
            replacement_start_frame=c.replacement_start_frame,
            auto_image_retry=(
                json.loads(c.auto_image_retry_json) if c.auto_image_retry_json else None
            ),  # v815
            qc=c._safe_qc(),  # v939 shadow-mode clip QC
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

    # v947 — capture the PRIOR state before we overwrite it. Re-clicking approve
    # on an already-approved clip must not re-fire the auto-finish trigger: the
    # export may already be DONE, and _queue_export_run's join only covers
    # queued/running, so it would start a second full export. A genuine redo
    # (reject -> redo -> approve) resets the status, so this stays False there
    # and the trigger fires as it should.
    was_approved = clip.approval_status == "approved"

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

    # v947 — auto-finish: the build declared its whole finish; the last
    # approval is the go. Never blocks the approval itself.
    if not was_approved:
        try:
            _maybe_auto_finish_export(db, job)
        except Exception as _af:
            # The approval is already committed. What is still open is the
            # TRIGGER's own transaction, and leaving it deactivated would make
            # the ApprovalResponse below raise PendingRollbackError the moment
            # it touches an expired attribute — a 500 on an approval that
            # actually succeeded. Roll back the failed trigger, not the approval.
            db.rollback()
            print(f"[AutoFinish] job={clip.job_id[:8]} trigger error "
                  f"(approval unaffected): {_af}", flush=True)

    return ApprovalResponse(
        clip_id=clip.id,
        status="approved",
        message="Clip approved" + (" - next clip will start generating" if next_clip_triggered else ""),
        attempts_remaining=UNLIMITED_ATTEMPTS_REMAINING
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
        attempts_remaining=UNLIMITED_ATTEMPTS_REMAINING
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
    error_reason: Optional[str] = None  # v815 — worker-supplied reason (PROMINENT_PEOPLE / CELEBRITY)


def _swap_clip_start_frame(clip, new_key):
    """v815 — point a clip at a new start_frame R2 key and re-queue it for
    the worker redo poll. Shared by manual replace-image + auto-retry.
    Does NOT bump generation_attempt (image substitution is not a same-image
    retry), so the 3-attempt cap does not limit a sweep."""
    clip.start_frame = new_key
    clip.error_code = None
    clip.error_message = None
    clip.status = ClipStatus.FLOW_REDO_QUEUED.value
    clip.approval_status = "pending_review"
    clip.claimed_by_worker = None
    clip.claimed_at = None


def clip_owner_user_id(db, clip):
    """v815 — resolve the account that owns a clip (via its job)."""
    job = db.query(Job).filter(Job.id == clip.job_id).first()
    return job.user_id if job else None


def _persist_retry_audit(clip, original, used, tried, count, mode):
    clip.auto_image_retry_json = json.dumps({
        "original_frame": original, "used_frame": used,
        "tried": tried, "count": count, "mode": mode,
    })


def _auto_image_retry(db, clip, rejected_key):
    """v815 — attempt a prominent-people auto-substitution. Returns
    {used_frame, mode, count} when a substitute was applied (clip now
    FLOW_REDO_QUEUED), or None to fall through to the manual replace card.
    A/B = single-shot; C ('batch') = bounded sweep (one substitute per call;
    the next worker rejection report calls this again for the next untried
    frame). 'tried' history persists in clip.auto_image_retry_json."""
    owner_id = clip_owner_user_id(db, clip)
    user = db.query(User).filter(User.id == owner_id).first() if owner_id else None
    mode = parse_auto_image_retry_mode(user.settings_json if user else None)
    if mode == "off":
        return None
    try:
        audit = json.loads(clip.auto_image_retry_json) if clip.auto_image_retry_json else {}
    except Exception:
        audit = {}
    original = audit.get("original_frame") or rejected_key
    tried = list(audit.get("tried") or [])
    if rejected_key and rejected_key not in tried:
        tried.append(rejected_key)
    # A/B single-shot: already substituted once -> yield to manual.
    if mode in ("next", "prev") and audit.get("count", 0) >= 1:
        _persist_retry_audit(clip, original, audit.get("used_frame"), tried, audit.get("count", 0), mode)
        return None
    job_clips = db.query(Clip).filter(Clip.job_id == clip.job_id).all()
    frames = order_distinct_frames(job_clips)
    # C with no other image -> fall back to A (next) per operator decision.
    eff_mode = mode
    if mode == "batch" and len([f for f in frames if f != original]) == 0:
        eff_mode = "next"
    cand = pick_substitute(eff_mode, frames, original, tried)
    if not cand:
        _persist_retry_audit(clip, original, audit.get("used_frame"), tried, audit.get("count", 0), mode)
        return None  # exhausted -> manual card
    new_count = audit.get("count", 0) + 1
    _swap_clip_start_frame(clip, cand)
    _persist_retry_audit(clip, original, cand, tried, new_count, mode)
    db.commit()
    print(f"[v815] auto-image-retry clip {clip.id} mode={mode} eff={eff_mode} "
          f"original={original} -> used={cand} count={new_count}", flush=True)
    return {"used_frame": cand, "mode": mode, "count": new_count}


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
        # v815 — swap via shared helper (manual replace + auto-retry both use
        # it). v701h — the helper sets status = flow_redo_queued so the
        # worker's /local-worker/clips/redo-pending poll picks the clip up
        # (PENDING would sit forever; redo-pending filters on FLOW_REDO_QUEUED).
        _swap_clip_start_frame(clip, new_key)
        clip.replacement_start_frame = previous_rejected  # keep audit
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
    # v892.5 — the POLICY-FALLBACK twin. prompt_text was patchable and its
    # Prompt-B twin was not, so an operator could correct a clip's prompt and
    # still have a policy-blocked retry render the superseded text from B.
    # Found on clip 14286, whose prompt_text was corrected to the speaking
    # prompt while prompt_text_b still held the silent composite-plate text.
    prompt_text_b: Optional[str] = None
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
    "dialogue_text", "dialogue_pad", "prompt_text", "prompt_text_b", "cut_mode",
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
        if cr not in ("single", "visual_pair", "audio_pair",
                      "composite_key", "composite_plate"):
            raise HTTPException(
                400,
                f"Unrecognized clip_role {req.clip_role!r}; expected one of "
                f"'single', 'visual_pair', 'audio_pair', 'composite_key', "
                f"'composite_plate'.",
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
    # v861 — 10s joined the set (Flow's 2026-07 composer). The Veo API path
    # folds 10→8 at render time; Flow renders a real 10s clip.
    if req.veo_render_duration_s is not None:
        if int(req.veo_render_duration_s) not in ALLOWED_CLIP_DURATIONS_S:
            raise HTTPException(
                400,
                f"veo_render_duration_s {req.veo_render_duration_s} not in "
                f"Veo render buckets {ALLOWED_CLIP_DURATIONS_S}",
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
    if req.prompt_text_b is not None:  # v892.5
        clip.prompt_text_b = req.prompt_text_b
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
                # v892.5 — prompt_text_b belongs in this list too. It was
                # applied above but omitted here, so a successful patch
                # reported `changed_fields: []` and read as a no-op. One more
                # hand-maintained enumeration drifting from the thing it
                # describes — the same disease as v892.2, in the response.
                "dialogue_text", "dialogue_pad", "prompt_text", "prompt_text_b",
                "clip_mode",
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
            _clear_clip_qc(clip)  # v939: an unscored take just joined this clip

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

            # v825 — recompute the job's completed_clips so a MANUALLY uploaded
            # clip is RECOGNIZED (the DONE tri-counter + progress bar + job
            # status). Every other completion path bumps this (worker uploads
            # @ ~12164 / ~13666, attach @ ~8134); upload_clip_variant did not,
            # so an operator-uploaded clip stayed uncounted — DONE stuck below
            # TOTAL and the job never flipped to completed (owner report
            # 2026-07-08). The autoflush before .count() includes the clip.status
            # = COMPLETED set just above (same pattern the worker paths rely on).
            job = db.query(Job).filter(Job.id == job_id).first()
            if job:
                completed = db.query(Clip).filter(
                    Clip.job_id == job_id,
                    Clip.status == ClipStatus.COMPLETED.value,
                ).count()
                job.completed_clips = completed
                total = job.total_clips or 0
                if total > 0:
                    job.progress_percent = int((completed / total) * 100)
                    if completed >= total:
                        job.status = "completed"
                        job.completed_at = _dt.utcnow()
                print(
                    f"[v825 upload-variant] job {job_id[:8]} completed_clips -> "
                    f"{completed}/{total} after clip {clip_index + 1} user upload",
                    flush=True,
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


class RecreateClipRequest(BaseModel):
    clip_index: int


@app.post("/api/jobs/{job_id}/clips/recreate")
async def recreate_deleted_clip(
    job_id: str,
    request: RecreateClipRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v932 — rebuild ONE deleted clip row from the job's own dialogue_json.

    A clip delete is a hard delete (row + output file). Before this endpoint
    the only recovery was promoting the whole batch again — a brand-new job
    and a full re-render of every clip. The Job already carries everything
    the row needs: dialogue_json lines[clip_index] has the line text, the
    v572 Veo prompt override, Prompt B (v805/v821), the v861 durations and
    start_image_idx; frames_storage_keys maps start_image_idx to the frame
    key. Rebuild the row from that and queue it as flow_redo_queued so the
    worker's redo path re-renders exactly this one clip.

    Deliberately narrow scope — refuse loudly rather than mis-build:
      - flow-backend jobs only (pickup rides the flow redo path)
      - main timeline clips only (no audio twins >= 100000, no plates)
      - fresh/cut clips only (blend end-frames and v718i explicit end-frame
        bindings need neighbor-clip context this path does not rebuild)
    """
    job = get_user_job(db, job_id, current_user)

    if job.backend != 'flow':
        raise HTTPException(status_code=400, detail=f"Recreate is only supported for flow jobs (this job backend: {job.backend})")

    ci = request.clip_index

    try:
        dialogue_data = json.loads(job.dialogue_json) if job.dialogue_json else {}
    except (json.JSONDecodeError, TypeError):
        dialogue_data = {}
    lines = dialogue_data.get("lines") or []

    if ci < 0 or ci >= len(lines):
        raise HTTPException(
            status_code=400,
            detail=f"clip_index {ci} is outside this job's dialogue lines (0..{len(lines) - 1}). "
                   f"Audio twins (100000+) and composite plates (200000+) cannot be recreated here."
        )

    existing = db.query(Clip).filter(Clip.job_id == job_id, Clip.clip_index == ci).first()
    if existing:
        raise HTTPException(status_code=409, detail=f"Clip index {ci} already exists on this job (clip id {existing.id}, status {existing.status})")

    line = lines[ci] if isinstance(lines[ci], dict) else {}

    clip_mode = line.get('clip_mode', 'fresh') or 'fresh'
    if clip_mode == 'blend' or line.get('end_frame_image_node_id'):
        raise HTTPException(
            status_code=400,
            detail="Recreate supports fresh/cut clips only — blend and explicit end-frame clips "
                   "need neighbor-frame context this path does not rebuild."
        )
    # v932.2 — a composite scene (v892) renders as a key clip PAIRED to a
    # plate sibling at 200000+ci; recreating just the key row here would drop
    # the pairing and the plate would stay orphaned on the deleted id.
    if line.get('composite_plate_image_node_id'):
        raise HTTPException(
            status_code=400,
            detail="This line is a composite scene (v892 key+plate pair) — recreate cannot rebuild "
                   "the pair. Re-promote the batch to recover it."
        )

    # Start frame: same derivation as the create path (main.py Phase 2) and
    # the redo-pending fallback — sorted frames_storage_keys + start_image_idx.
    start_frame_key = None
    try:
        frames_keys = json.loads(job.frames_storage_keys) if job.frames_storage_keys else {}
    except (json.JSONDecodeError, TypeError):
        frames_keys = {}
    uploaded_frames = sorted(frames_keys.keys())
    if uploaded_frames:
        start_img_idx = line.get('start_image_idx', 0) or 0
        start_frame_key = f"jobs/{job_id}/frames/{uploaded_frames[start_img_idx % len(uploaded_frames)]}"
    if not start_frame_key:
        raise HTTPException(status_code=409, detail="Job has no stored frames (frames_storage_keys empty) — cannot rebuild the start frame.")

    # Prompt: when the line carries a v572 override, compose it exactly as the
    # promote path does. No override → NULL; the flow worker's legacy prompt
    # builder handles that case (same contract as image_platform promote).
    prompt_text = None
    _override = (line.get('veo_prompt_override') or '').strip() or None
    if _override:
        try:
            from veo_prompt_overrides import compose_final_prompt
            prompt_text = compose_final_prompt(_override, line.get('veo_negative_prompt_override'))
        except Exception as _ce:
            print(f"[v932] compose_final_prompt failed for job {job_id} clip {ci}: {_ce}", flush=True)
            prompt_text = _override

    warnings = []
    _cutoff = job_age_cutoff()
    if _cutoff is not None and job.created_at and job.created_at < _cutoff:
        warnings.append(
            "Job is older than the worker claim window (WORKER_MAX_JOB_AGE_DAYS, default 7 days) — "
            "the worker will NOT pick this clip up until that window is raised."
        )
    if (line.get('clip_role') or '').lower() == 'visual_pair':
        twin = db.query(Clip).filter(Clip.job_id == job_id, Clip.clip_index == 100000 + ci).first()
        if twin is None:
            warnings.append(
                f"This line is a v698A visual_pair but its audio twin (clip_index {100000 + ci}) "
                f"is also missing — this endpoint does not recreate audio twins."
            )

    clip = Clip(
        job_id=job_id,
        clip_index=ci,
        dialogue_id=ci + 1,
        dialogue_text=line.get('text', '') or '',
        dialogue_pad=line.get('dialogue_pad'),
        status=ClipStatus.FLOW_REDO_QUEUED.value,
        clip_mode=clip_mode,
        scene_index=line.get('scene_index', 0) or 0,
        cut_mode=line.get('cut_mode'),
        target_duration_s=line.get('target_duration_s'),
        veo_render_duration_s=line.get('veo_render_duration_s'),
        caption=line.get('caption'),
        scene_type=line.get('scene_type'),
        bg_color=line.get('bg_color'),
        clip_role=line.get('clip_role'),
        voiceover_anchor_image_node_id=line.get('voiceover_anchor_image_node_id'),
        voiceover_line=line.get('voiceover_line'),
        start_frame=start_frame_key,
        prompt_text=prompt_text,
        prompt_text_b=line.get('veo_prompt_b'),
        dialogue_text_b=line.get('veo_prompt_b_line'),
        generation_attempt=1,
        use_logged_params=False,
        redo_reason="v932 recreate: clip row rebuilt after delete",
    )
    db.add(clip)

    # Bump job bookkeeping: total_clips counts rows; updated_at keeps the job
    # inside the redo-pending 24h freshness filter.
    db.flush()
    job.total_clips = db.query(Clip).filter(Clip.job_id == job_id).count()
    job.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(clip)

    add_job_log(db, job_id, f"Clip {ci + 1} recreated from dialogue_json and queued for render (v932)", "INFO", "recreate")
    # v932 TEMP DIAG — remove after the first operator-confirmed recreate+render.
    print(f"[v932 TEMP] recreate job={job_id} clip_index={ci} clip_id={clip.id} "
          f"start_frame={start_frame_key} prompt={'override' if prompt_text else 'NULL(legacy-build)'} "
          f"dur={clip.veo_render_duration_s} warnings={len(warnings)}", flush=True)

    return {
        "success": True,
        "clip_id": clip.id,
        "clip_index": ci,
        "status": clip.status,
        "start_frame": start_frame_key,
        "prompt_attached": bool(prompt_text),
        "veo_render_duration_s": clip.veo_render_duration_s,
        "warnings": warnings,
    }


class AddVoiceoverRequest(BaseModel):
    voiceover_line: str
    voiceover_line_b: Optional[str] = None   # v821 reworded line for Prompt B
    audio_prompt: Optional[str] = None       # v789 authored twin prompt (verbatim when given)
    audio_prompt_b: Optional[str] = None     # v821 Prompt B for the twin


@app.post("/api/jobs/{job_id}/clips/{clip_index}/add-voiceover")
async def add_voiceover_to_clip(
    job_id: str,
    clip_index: int,
    request: AddVoiceoverRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v933 — turn an existing silent clip into a v698A voiceover pair on the
    LIVE job, without a re-promote.

    The visual clip's render is kept untouched; this only (1) stamps the
    v698A pairing fields on the visual row, (2) creates the missing
    audio_pair twin at clip_index+100000 (anchor frame + node id copied from
    the job's existing twins, so the voice/face anchor stays identical),
    (3) wires paired_clip_id both ways, (4) updates dialogue_json so future
    recreates/exports see the line, and (5) queues ONLY the new twin for
    render on the flow redo lane ('v933 modify' marker — exempt from the
    job age cap like v932 recreates, still inside the 24h freshness bound).

    Needs at least one existing audio_pair on the job to anchor from —
    a job that never had voiceover pairs must be re-promoted instead.
    """
    job = get_user_job(db, job_id, current_user)
    if job.backend != 'flow':
        raise HTTPException(status_code=400, detail=f"add-voiceover is only supported for flow jobs (this job backend: {job.backend})")

    line_text = (request.voiceover_line or "").strip()
    if not line_text:
        raise HTTPException(status_code=400, detail="voiceover_line is empty")

    visual = db.query(Clip).filter(Clip.job_id == job_id, Clip.clip_index == clip_index).first()
    if visual is None:
        raise HTTPException(status_code=404, detail=f"No clip at clip_index {clip_index} on this job")
    if (visual.clip_role or '') not in ('', None) and visual.clip_role != 'visual_pair':
        raise HTTPException(status_code=400, detail=f"Clip {clip_index} has clip_role={visual.clip_role} — only a plain silent clip (or an unpaired visual_pair) can gain a voiceover")

    twin_index = 100000 + clip_index
    existing_twin = db.query(Clip).filter(Clip.job_id == job_id, Clip.clip_index == twin_index).first()
    if existing_twin:
        raise HTTPException(status_code=409, detail=f"Audio twin already exists at clip_index {twin_index} (clip id {existing_twin.id}, status {existing_twin.status})")

    # Anchor template: any existing audio twin on this job carries the anchor
    # frame key + anchor node id the whole video uses.
    template = db.query(Clip).filter(
        Clip.job_id == job_id,
        Clip.clip_role == 'audio_pair',
        Clip.start_frame.isnot(None),
    ).order_by(Clip.clip_index.asc()).first()
    if template is None:
        raise HTTPException(status_code=409, detail="Job has no existing audio_pair twin to copy the anchor frame from — re-promote the batch instead.")

    # Twin prompt: authored prompt wins (v789 verbatim). Fallback: clone the
    # template twin's prompt and swap the quoted spoken span (v872 — the line
    # is the only double-quoted span in the prompt).
    import re as _re
    def _swap_quoted_line(prompt: str, new_line: str) -> Optional[str]:
        if not prompt:
            return None
        swapped, n = _re.subn(r'"[^"]+"', '"' + new_line + '"', prompt, count=1)
        return swapped if n == 1 and prompt.count('"') == 2 else None

    audio_prompt = (request.audio_prompt or "").strip() or _swap_quoted_line(template.prompt_text or "", line_text)
    if not audio_prompt:
        raise HTTPException(status_code=409, detail="No audio_prompt given and the template twin's prompt could not be line-swapped — pass audio_prompt explicitly.")
    line_b = (request.voiceover_line_b or "").strip() or None
    audio_prompt_b = (request.audio_prompt_b or "").strip() or (
        _swap_quoted_line(audio_prompt, line_b) if line_b else None
    )

    # (1) visual side of the pair
    visual.clip_role = 'visual_pair'
    visual.voiceover_line = line_text
    visual.voiceover_anchor_image_node_id = template.voiceover_anchor_image_node_id
    visual.dialogue_text = line_text

    # (2) the audio twin — same field shape as v698A Phase 3a/3b
    twin = Clip(
        job_id=job_id,
        clip_index=twin_index,
        dialogue_id=visual.dialogue_id,
        dialogue_text=line_text,
        dialogue_text_b=line_b,
        status=ClipStatus.FLOW_REDO_QUEUED.value,
        scene_index=visual.scene_index,
        clip_role='audio_pair',
        paired_clip_id=visual.id,
        voiceover_anchor_image_node_id=template.voiceover_anchor_image_node_id,
        voiceover_line=line_text,
        cut_mode='auto',
        scene_type='shot',
        start_frame=template.start_frame,
        prompt_text=audio_prompt,
        prompt_text_b=audio_prompt_b,
        generation_attempt=1,
        use_logged_params=False,
        redo_reason="v933 modify: audio twin added after promote",
    )
    db.add(twin)
    db.flush()
    # (3) bidirectional pairing, matching Phase 3a
    visual.paired_clip_id = twin.id

    # (4) dialogue_json so recreates/exports see the new line
    try:
        dialogue_data = json.loads(job.dialogue_json) if job.dialogue_json else {}
        lines = dialogue_data.get("lines") or []
        if 0 <= clip_index < len(lines) and isinstance(lines[clip_index], dict):
            anchor_local_idx = None
            for _l in lines:
                if isinstance(_l, dict) and _l.get("voiceover_anchor_image_local_index") is not None:
                    anchor_local_idx = _l.get("voiceover_anchor_image_local_index")
                    break
            lines[clip_index].update({
                "text": line_text,
                "clip_role": "visual_pair",
                "voiceover_line": line_text,
                "voiceover_anchor_image_node_id": template.voiceover_anchor_image_node_id,
                "voiceover_anchor_image_local_index": anchor_local_idx,
                "voiceover_audio_prompt_override": audio_prompt,
                "veo_prompt_b_line": line_b,
            })
            job.dialogue_json = json.dumps(dialogue_data)
    except (json.JSONDecodeError, TypeError) as _dj_err:
        print(f"[v933] dialogue_json update skipped for job {job_id}: {_dj_err}", flush=True)

    job.total_clips = db.query(Clip).filter(Clip.job_id == job_id).count()
    job.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(twin)

    add_job_log(db, job_id, f"Clip {clip_index + 1} gained a voiceover — audio twin {twin_index} created and queued (v933)", "INFO", "modify")
    # v933 TEMP DIAG — remove after the first operator-confirmed twin render.
    print(f"[v933 TEMP] add-voiceover job={job_id} visual={visual.id} twin={twin.id} "
          f"anchor_frame={template.start_frame} prompt={'authored' if request.audio_prompt else 'template-swap'} "
          f"prompt_b={'yes' if audio_prompt_b else 'NO'}", flush=True)

    return {
        "success": True,
        "visual_clip_id": visual.id,
        "twin_clip_id": twin.id,
        "twin_clip_index": twin_index,
        "twin_status": twin.status,
        "anchor_start_frame": template.start_frame,
        "prompt_source": "authored" if request.audio_prompt else "template-swap",
        "prompt_b_attached": bool(audio_prompt_b),
    }


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
    _clear_clip_qc(clip)  # v939: the report may point at a deleted take
    
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
            attempts_remaining=UNLIMITED_ATTEMPTS_REMAINING,
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
    - Attempt 3+: Uses fresh parameters (no log)
    - v931: attempts are unlimited (the old cap at 3 is removed)
    
    For Flow backend jobs: sets status to 'flow_redo_queued' (handled by Flow worker)
    For API backend jobs: sets status to 'redo_queued' (handled by API worker)
    """
    clip = get_user_clip(db, clip_id, current_user)

    # v945.8 (Codex rev 536 blocker 5) — charswap clips must NEVER be redone.
    # Every redo lane rebuilds the prompt/payload WITHOUT the charswap
    # metadata and submits a plain image-to-video render of the start frame —
    # a wrong-path render that looks completed (measured twice, 2026-08-27,
    # both hand-cancelled mid-race). Refuse at the door; the correct move is
    # recreating the job (prepare-for-video -> POST /api/jobs).
    if (getattr(clip, "render_method", None) or "").strip().lower() == "charswap":
        raise HTTPException(
            400,
            "This is a charswap clip — redo would re-render it WITHOUT the "
            "swap (wrong-path render). Recreate the job instead "
            "(Prepare for video -> create job); v945.8.")

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
            attempts_remaining=UNLIMITED_ATTEMPTS_REMAINING
        )
    
    if clip.status == ClipStatus.GENERATING.value:
        raise HTTPException(status_code=400, detail="Clip is currently generating - please wait")
    
    if clip.status == ClipStatus.PENDING.value:
        raise HTTPException(status_code=400, detail="Clip is pending initial generation")
    
    # Allow redo for completed or failed clips
    if clip.status not in [ClipStatus.COMPLETED.value, ClipStatus.FAILED.value]:
        raise HTTPException(status_code=400, detail=f"Can only redo completed or failed clips (current status: {clip.status})")
    
    # v931 — redos are unlimited. The old 3-attempt cap ('max_attempts' flag +
    # MAX_ATTEMPTS_REACHED 400) is gone. Clear the legacy flag so clips capped
    # under the old rule become reviewable again after this redo.
    if clip.approval_status == "max_attempts":
        clip.approval_status = "pending_review"
    
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
    _clear_clip_qc(clip)  # v939: the scored takes are no longer the whole set
    
    # Determine if we use logged params
    # Attempt 2: use logged params (same settings)
    # Attempt 3+: fresh generation (no logged params)
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
    
    # v892.4 — captured before the rebuild below can overwrite it.
    _pre_redo_prompt = clip.prompt_text
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

    # === v892.4 — an AUTHORED Veo prompt survives a redo ===
    # A build ships its own per-clip Veo prompt (v572 veo_prompt_override) and
    # promote copies it into Clip.prompt_text. The regeneration above rebuilds
    # the prompt from scratch on EVERY redo, which threw that authored text away
    # and replaced it with generic build_prompt output — whose default body says
    # "speaks directly to camera". Operator 2026-08-18, iterating a street scene:
    # the redone clip kept addressing the lens and kept the superseded motion,
    # because none of the authored text was in play any more. The untouched
    # clips in the same job still carried theirs, which is what proved it.
    #
    # So: if this clip is AUTHORED (the job's dialogue_json carries an override
    # for it), keep the prompt it already had — including one the operator just
    # PATCHed in, which is how a single clip gets iterated without re-promoting
    # all of them. A redo that supplies new_dialogue still takes the rebuilt
    # prompt, and a clip with no authored override is untouched by this.
    if _pre_redo_prompt and not (request and request.new_dialogue is not None):
        try:
            _dj = json.loads(job.dialogue_json) if job.dialogue_json else {}
            _lines = _dj.get("lines") if isinstance(_dj, dict) else _dj
            _authored = False
            for _ln in (_lines or []):
                if not isinstance(_ln, dict):
                    continue
                if _ln.get("id") == clip.clip_index + 1 or _ln.get("clip_index") == clip.clip_index:
                    _authored = bool((_ln.get("veo_prompt_override") or "").strip())
                    break
            if _authored and clip.prompt_text != _pre_redo_prompt:
                clip.prompt_text = _pre_redo_prompt
                # TEMP DIAGNOSTIC (v892.4) — strip once seen on a live redo.
                print(f"[v892.4 REDO] clip {clip.id}: authored prompt kept "
                      f"({len(_pre_redo_prompt)} chars); discarded the rebuild",
                      flush=True)
        except Exception as _ae:
            print(f"[v892.4 REDO] clip {clip.id}: authored-prompt check failed: {_ae}",
                  flush=True)

    
    # Part C: Add debug log to prove DB state at redo time
    add_job_log(
        db, job.id,
        f"[RedoDebug] clip={clip.id} backend={job.backend} flow_url={'yes' if job.flow_project_url else 'no'} -> status={clip.status}",
        "DEBUG", "redo"
    )
    
    db.commit()
    
    add_job_log(
        db, clip.job_id, 
        f"Clip {clip.clip_index + 1} redo requested (attempt {new_attempt}, {'with' if clip.use_logged_params else 'without'} logged params, backend={job.backend})",
        "INFO", "approval",
        details={"reason": request.reason if request else None, "use_logged_params": clip.use_logged_params, "backend": job.backend}
    )
    
    return ApprovalResponse(
        clip_id=clip.id,
        status="redo_queued",  # UI always sees "redo_queued" for display purposes
        message=f"Redo queued (attempt {new_attempt}). {'Using same parameters.' if clip.use_logged_params else 'Using fresh parameters.'}",
        attempts_remaining=UNLIMITED_ATTEMPTS_REMAINING
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
        "attempts_remaining": UNLIMITED_ATTEMPTS_REMAINING,
    }


@app.post("/api/clips/{clip_id}/qc")
async def set_clip_qc(
    clip_id: int,
    req: ClipQCRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v939 — store the shadow-mode QC report for a clip.

    Written by code/clip_qc.py from the operator's box. It records whether each
    rendered take actually SAID its whole line and it decides nothing: v886.3
    is untouched, the operator still approves every clip. Overwrites any
    previous report, because the latest scoring of the same audio wins.

    Mirrors image_platform.set_node_qc deliberately, including the 409: a clip
    mid-render is about to replace the take a report would be describing, so a
    report accepted now would be stale the moment it lands.
    """
    clip = get_user_clip(db, clip_id, current_user)

    if clip.status in (ClipStatus.PENDING.value, ClipStatus.GENERATING.value):
        raise HTTPException(
            status_code=409,
            detail=f"Clip is {clip.status} - rescore after it lands")

    rep = req.report
    if rep.get("version") != 1:
        raise HTTPException(status_code=422, detail="qc report must carry version: 1")
    takes = rep.get("takes")
    if takes is not None and not isinstance(takes, list):
        raise HTTPException(status_code=422, detail="takes must be a list")

    rec = rep.get("recommended_attempt")
    if rec is not None:
        # bool is an int subclass, so JSON `true` would otherwise resolve to
        # attempt 1. Floats are rejected rather than silently truncated.
        if isinstance(rec, bool) or not isinstance(rec, (int, str)):
            raise HTTPException(status_code=422,
                                detail="recommended_attempt must be an integer")
        try:
            rec = int(rec)
        except (TypeError, ValueError):
            raise HTTPException(status_code=422,
                                detail="recommended_attempt must be an integer")
        rep["recommended_attempt"] = rec
        # The recommendation must name a take that exists on THIS clip,
        # otherwise the agreement metric compares against a ghost.
        known = {v.get("attempt") for v in
                 (json.loads(clip.versions_json) if clip.versions_json else [])}
        known.add(clip.generation_attempt or 1)
        if rec not in known:
            raise HTTPException(status_code=422,
                                detail="recommended_attempt is not a take on this clip")

    blob = json.dumps(rep)
    if len(blob) > 64_000:
        raise HTTPException(status_code=413,
                            detail=f"qc report too large ({len(blob)} bytes, cap 64000)")
    clip.qc_json = blob
    db.commit()

    # v939 TEMP diagnostic — remove once operator-side evidence confirms reports
    # are landing and rendering in the review UI.
    print(f"[ClipQC/v939 TEMP] clip {clip_id} scored: verdict={rep.get('verdict')} "
          f"recommended={rec} takes={len(takes or [])}", flush=True)
    return {"ok": True, "clip_id": clip_id, "verdict": rep.get("verdict")}


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
        "attempts_remaining": UNLIMITED_ATTEMPTS_REMAINING,
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
    from auth import validate_bearer_token

    output_dir = None
    with get_db() as _db:
        # Authenticate
        if GOOGLE_AUTH_ENABLED:
            # v886 parity: this handler authenticates by hand (no dependency
            # injection, see note above), so the bearer path never ran here —
            # CLI clients got 401 on downloads while every JSON endpoint
            # accepted the same token. Bearer first, cookie fallback.
            user = validate_bearer_token(request, _db)
            if user is None:
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
        # v872 — stream to disk; never hold the clip in RAM (see _spool_upload_to_path)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4', dir=str(output_dir))
        tmp.close()
        size = await asyncio.to_thread(_spool_upload_to_path, clip_file, tmp.name)
        if size == 0:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass
            continue
        clip_files.append({
            'temp_path': tmp.name,
            'original_filename': clip_file.filename or f"clip_{i}.mp4",
            'index': i,
            'size': size,
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
        # v872 — stream to disk; never hold the clip in RAM (see _spool_upload_to_path)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4', dir=str(output_dir))
        tmp.close()
        size = await asyncio.to_thread(_spool_upload_to_path, clip_file, tmp.name)
        if size == 0:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass
            continue
        clip_data.append({
            'temp_path': tmp.name,
            'original_filename': clip_file.filename or f"clip_{i}.mp4",
            'index': i,
            'size': size,
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
        _clear_clip_qc(db_clip)  # v939: this render replaced the scored one
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
    # v825 — guard: never flip to completed on total_clips 0/None (false-flip / None-compare)
    if job.total_clips and completed >= job.total_clips:
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
# ExportSettings moved to finishing_models.py (v947) — imported at top of file.

MEDIA_STAGE_PREFIX = "media-staging"
MEDIA_STAGE_MAX_BYTES = 500 * 1024 * 1024
MEDIA_STAGE_EXT = {".mp4", ".mov", ".m4v", ".webm", ".jpg", ".jpeg", ".png"}
# Longest a staged object may live. Matches the maximum expires_in, so nothing
# outlives the URL that could reach it.
MEDIA_STAGE_MAX_AGE_S = 86400


def _media_stage_key(user_id: str, stage_id: str, suffix: str) -> str:
    """User-scoped so one caller can never presign or delete another's file.

    The epoch prefix makes a key self-dating: list_objects returns keys only,
    with no LastModified, so without it there is no way to find stale objects
    without a HEAD per key."""
    # main.py has no bare module-level `time` (only `import time as _time_v872`)
    # and no `re` either — every stdlib name here must be imported locally or
    # it resolves at CALL time into a NameError. See v901.1.
    import time as _t
    return f"{MEDIA_STAGE_PREFIX}/{user_id}/{int(_t.time())}-{stage_id}{suffix}"


def _media_stage_age_s(key: str):
    """Seconds since this key was staged, or None if it predates the epoch
    format (2026-08-22) and cannot be dated."""
    import time as _t
    tail = key.rsplit("/", 1)[-1]
    stamp, sep, _ = tail.partition("-")
    if not sep or not stamp.isdigit():
        return None
    return max(0, int(_t.time()) - int(stamp))


def _sweep_stale_stage_objects(storage, user_id: str) -> int:
    """Delete this user's staged objects that are past the maximum lifetime.

    A presigned URL expires but the OBJECT does not, so a caller that dies
    between staging and cleanup leaves a file behind forever. Rather than
    depend on a bucket lifecycle rule that lives outside this repo (and that
    nobody here has credentials to set), every stage request sweeps the
    caller's own prefix. Cheap: one list scoped to one user, normally 0-2 keys.

    Never raises — a failed sweep must not fail the upload the user asked for.
    """
    removed = 0
    try:
        for key in storage.list_objects(f"{MEDIA_STAGE_PREFIX}/{user_id}/", max_keys=1000):
            age = _media_stage_age_s(key)
            if age is None or age <= MEDIA_STAGE_MAX_AGE_S:
                continue          # undatable (pre-2026-08-22) or still current
            try:
                storage.delete(key)
                removed += 1
            except Exception as exc:                              # noqa: BLE001
                print(f"[MediaStage] sweep could not delete {key}: {exc}", flush=True)
    except Exception as exc:                                      # noqa: BLE001
        print(f"[MediaStage] sweep failed for {user_id[:8]}: {exc}", flush=True)
    if removed:
        print(f"[MediaStage] swept {removed} stale object(s) for {user_id[:8]}",
              flush=True)
    return removed


@app.post("/api/media/stage")
async def stage_media(
    file: UploadFile = File(...),
    expires_in: int = Form(3600),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Give any file a short-lived public URL, without the caller owning storage.

    Publishing a reel through Instagram's Business Login route — or through a
    broker like Blotato — requires the video to sit at a URL the remote service
    can fetch ANONYMOUSLY. Job outputs already get that from the presigned-R2
    redirect on /api/jobs/{id}/outputs/{name}. A file the platform has never
    seen (a local edit, an experiment render) had no route at all, and the only
    workarounds were handing every user R2 credentials or pushing their video to
    a third-party file host. Neither is acceptable for a normal user.

    The URL is unguessable (uuid) and expires; the object is user-scoped and
    deletable via DELETE /api/media/stage/{stage_id}.
    """
    from backends.storage import is_storage_configured, get_storage

    if not is_storage_configured():
        raise HTTPException(status_code=503,
                            detail="Object storage is not configured on this server")

    suffix = Path(file.filename).suffix.lower() if file.filename else ""
    if suffix not in MEDIA_STAGE_EXT:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported type '{suffix}'. Allowed: {', '.join(sorted(MEDIA_STAGE_EXT))}")

    expires_in = max(60, min(int(expires_in or 3600), 86400))

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(content) > MEDIA_STAGE_MAX_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File is {len(content) / 1e6:.0f}MB; the cap is "
                   f"{MEDIA_STAGE_MAX_BYTES / 1e6:.0f}MB")

    stage_id = uuid.uuid4().hex
    key = _media_stage_key(current_user.id, stage_id, suffix)

    # Upload the bytes straight through. The earlier version staged them in a
    # temp file first and swallowed OSError on unlink, so any failure to remove
    # it leaked disk on a long-running server. There is no reason to touch the
    # local filesystem at all — the content is already in memory.
    storage = get_storage()
    # Sweep this caller's stale objects before adding another. Keeps the bucket
    # self-cleaning without depending on a lifecycle rule set outside the repo.
    await asyncio.to_thread(_sweep_stale_stage_objects, storage, current_user.id)
    await asyncio.to_thread(
        storage.upload_bytes, content, key,
        file.content_type or "application/octet-stream")
    try:
        url = storage.get_presigned_url(key, expires_in=expires_in)
    except Exception:
        # Presigning can fail after the object landed; do not orphan it.
        try:
            await asyncio.to_thread(storage.delete, key)
        except Exception:                                         # noqa: BLE001
            print(f"[MediaStage] orphaned {key} after presign failure", flush=True)
        raise

    print(f"[MediaStage] user={current_user.id[:8]} staged {key} "
          f"({len(content) / 1e6:.1f}MB, {expires_in}s)", flush=True)

    return {
        "stage_id": stage_id,
        "key": key,
        "url": url,
        "size_bytes": len(content),
        "expires_in": expires_in,
    }


@app.delete("/api/media/stage/{stage_id}")
async def unstage_media(
    stage_id: str,
    ext: str = Query("", description="file suffix used at stage time, e.g. .mp4"),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Delete a staged object. Scoped to the caller's own prefix, so a guessed
    stage_id belonging to another user resolves to a key that is not theirs and
    simply is not found."""
    # main.py has no module-level `re` — every other use in this file is a
    # function-local import. Relying on a global here raised NameError at call
    # time (500), which `import main` cannot catch because the body never runs.
    import re as _re
    from backends.storage import is_storage_configured, get_storage

    if not is_storage_configured():
        raise HTTPException(status_code=503, detail="Object storage is not configured")
    if not _re.fullmatch(r"[0-9a-f]{32}", stage_id or ""):
        raise HTTPException(status_code=400, detail="Bad stage_id")

    storage = get_storage()
    # Keys carry an epoch prefix, so they cannot be rebuilt from stage_id alone.
    # Listing the caller's own prefix and matching is both correct and cheaper
    # than the previous guess-every-extension loop — one call instead of seven.
    removed = []
    prefix = f"{MEDIA_STAGE_PREFIX}/{current_user.id}/"
    try:
        keys = await asyncio.to_thread(storage.list_objects, prefix, 1000)
    except Exception as exc:                                      # noqa: BLE001
        print(f"[MediaStage] list failed for {prefix}: {exc}", flush=True)
        keys = []
    for key in keys:
        # The prefix already scopes this to the caller, so a stage_id belonging
        # to someone else simply does not appear here.
        if stage_id not in key.rsplit("/", 1)[-1]:
            continue
        try:
            await asyncio.to_thread(storage.delete, key)
            removed.append(key)
        except Exception as exc:                                  # noqa: BLE001
            print(f"[MediaStage] delete failed {key}: {exc}", flush=True)

    print(f"[MediaStage] user={current_user.id[:8]} unstaged "
          f"{len(removed)} object(s) for {stage_id}", flush=True)
    return {"removed": removed}


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


@app.post("/api/jobs/{job_id}/upload-music")
async def upload_music(
    job_id: str,
    audio: UploadFile = File(...),
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v888 — upload a MUSIC BED for the export to lay under the finished cut.

    Deliberately separate from upload-master-audio: that one aligns clips to a
    spoken master and routes to export_with_master_audio. This is a score, and
    it works on ANY job type, not just assemble jobs.
    """
    job = get_user_job(db, job_id, current_user)

    allowed_ext = {'.mp3', '.wav', '.m4a', '.aac', '.ogg', '.flac'}
    suffix = Path(audio.filename).suffix.lower() if audio.filename else ''
    if suffix not in allowed_ext:
        raise HTTPException(status_code=400,
                            detail=f"Unsupported audio format. Allowed: {', '.join(allowed_ext)}")

    output_dir = Path(job.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    music_filename = f"music_bed{suffix}"
    music_path = output_dir / music_filename

    content = await audio.read()
    with open(music_path, "wb") as f:
        f.write(content)

    try:
        from backends.storage import is_storage_configured, get_storage
        if is_storage_configured():
            storage = get_storage()
            r2_key = f"jobs/{job_id}/outputs/{music_filename}"
            await asyncio.to_thread(storage.upload_file, str(music_path), r2_key,
                                    audio.content_type or 'audio/mpeg')
            print(f"[v888/Music] Uploaded to R2: {r2_key}", flush=True)
    except Exception as e:
        print(f"[v888/Music] R2 upload failed (non-fatal): {e}", flush=True)

    size_mb = len(content) / (1024 * 1024)
    print(f"[v888/Music] Saved {music_filename} ({size_mb:.1f}MB) for job {job_id[:8]}",
          flush=True)
    return {"filename": music_filename, "size_bytes": len(content)}


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


# ============================================================================
# v850 — DURABLE EXPORT QUEUE
# ----------------------------------------------------------------------------
# Pre-v850 the whole 5-15 min export ran inside POST /export-final. Render
# auto-deploys on every push to main; the SIGTERM killed ffmpeg mid-run, the
# mp4 never reached R2, and the browser's poll timed out with "Export polling
# exceeded the time cap". Nothing on the server remembered the export had even
# been asked for.
#
# Now: the POST persists an ExportRun row and returns 202. A detached task
# does the work and heartbeats every 30s. A sweeper (on boot + every 60s)
# re-runs any ExportRun whose heartbeat went stale — which is exactly what a
# deploy/OOM/crash leaves behind. Re-run is from scratch: the export is
# deterministic and mints a fresh final_export_<ts>_<hash>.mp4, so a partial
# file from the dead container can never be mistaken for the result.
# ============================================================================
import export_queue as _eq
from fastapi.responses import JSONResponse  # main.py's response imports bring in
                                            # FileResponse/StreamingResponse/
                                            # HTMLResponse/RedirectResponse, not this.

# Export ids this container is actively running. The sweeper never touches an
# id in here, so a slow-but-alive export is never double-started.
_LOCAL_EXPORT_IDS: set = set()
_EXPORT_TASKS: set = set()  # strong refs; asyncio only holds weak ones


def _spawn_export_runner(export_id: str) -> None:
    """Fire the runner detached from any HTTP request.

    Registers the id SYNCHRONOUSLY (before the task is even scheduled) so a
    sweeper tick landing in the gap can't decide the run is orphaned and start
    a second copy of the same 15-minute ffmpeg job.

    v854: the id is still registered BEFORE the task starts (that is the race
    guard), but a failure to create the task now UNREGISTERS it. It used to
    leak: callers on a worker thread hit "no running event loop" from
    create_task, the id stayed in _LOCAL_EXPORT_IDS with no task behind it, and
    every later sweep skipped that run forever. MUST be called from the event
    loop.
    """
    _LOCAL_EXPORT_IDS.add(export_id)
    try:
        task = asyncio.create_task(_export_runner(export_id))
    except RuntimeError as _cte:
        # No running loop == called from a thread. Never poison the set.
        _LOCAL_EXPORT_IDS.discard(export_id)
        print(f"[Export/v854] CANNOT SPAWN run={export_id[:8]} — {_cte}. "
              f"_spawn_export_runner must be called from the event loop.", flush=True)
        raise

    _EXPORT_TASKS.add(task)

    def _on_done(_t: "asyncio.Task") -> None:
        # Belt to the runner's own finally: if the coroutine dies BEFORE its
        # try block (an import, a bad arg), that finally never runs and the id
        # would leak — which is exactly how a run becomes unreclaimable.
        _EXPORT_TASKS.discard(_t)
        _LOCAL_EXPORT_IDS.discard(export_id)
        if _t.cancelled():
            return
        _exc = _t.exception()
        if _exc is not None:
            # Without this, an asyncio task's exception is swallowed until GC.
            print(f"[Export/v854] runner task DIED run={export_id[:8]}: "
                  f"{type(_exc).__name__}: {_exc}", flush=True)

    task.add_done_callback(_on_done)


def _next_queued_export_ids(limit: int) -> list:
    """Oldest queued runs first, skipping any this container already runs.

    Sync — call via to_thread. READ-ONLY: the CAS in _claim_export_run is what
    actually takes a row, so a dispatcher tick that races the sweeper is
    harmless (the loser gets rowcount=0 and returns).
    """
    from models import get_db, ExportRun

    if limit <= 0:
        return []
    with get_db() as _db:
        try:
            rows = (
                _db.query(ExportRun.id)
                .filter(ExportRun.state == _eq.STATE_QUEUED)
                .order_by(ExportRun.created_at.asc())
                .limit(limit + len(_LOCAL_EXPORT_IDS))
                .all()
            )
            out = [r[0] for r in rows if r[0] not in _LOCAL_EXPORT_IDS]
            return out[:limit]
        except Exception as _qe:
            print(f"[Export/v855] queue read failed: {_qe}", flush=True)
            return []


# v864 — memory guards for the v825 support-track step (see the call site in
# the export runner).
_V864_SUPPORT_LOCK = asyncio.Lock()  # module-level `import asyncio` (L20); 3.11 binds no loop at creation


def _v864_mem():
    """(avail_MB, rss_MB) — CONTAINER headroom, not the host's.

    v865 — this used to read /proc/meminfo MemAvailable. Inside a Render
    container that reports the HOST's free memory, which has nothing to do with
    the 2GB cgroup limit we are actually OOM-killed against: it returned tens of
    GB on a box that was seconds from being killed, so the guard below could
    never fire. mem_guard reads the cgroup limit first and only falls back to
    the host figure when there is genuinely no limit (local dev).
    """
    try:
        import mem_guard as _mg
        _s = _mg.snapshot()
        # A host-fallback number is NOT container headroom. Return None so the
        # caller treats headroom as unknown and proceeds rather than refusing
        # (or worse, trusting) a meaningless figure.
        return (_s["avail_mb"] if _s["source"] == "cgroup" else None), _s["rss_mb"]
    except Exception:
        return None, None


class UploadTooLarge(Exception):
    """Raised by _spool_upload_to_path when a capped upload runs past its cap.

    A plain exception rather than an HTTPException because the spooler is
    shared by routes that answer with different status codes; the route that
    set the cap decides what the caller is told.
    """

    def __init__(self, limit_bytes):
        super().__init__(f"upload exceeds {limit_bytes} bytes")
        self.limit_bytes = limit_bytes


def _spool_upload_to_path(upload_file, dst_path, chunk_bytes=1 << 20,
                          max_bytes=None) -> int:
    """v872 — stream an UploadFile to disk. Returns bytes written. SYNC: call
    via asyncio.to_thread.

    v943 — `max_bytes` caps the copy. It is checked WHILE copying, not after:
    a cap that only looks at the finished file has already let the whole
    upload onto the disk, which is the thing the cap exists to prevent. Left
    at None the copy is the original unbounded one, byte for byte.

    `await file.read()` materialises the ENTIRE upload as one bytes object.
    Starlette has already spooled anything over 1MB to a temp file, so that
    read buys nothing and costs a full copy in RAM — for a 20-clip attach that
    is up to 2GB of transient allocation, and the workers upload finished mp4s
    while an export is running (see the be09f595 log: upload-video POSTs
    interleaved with the concat phase, at 95%+ of the cgroup limit). Copying in
    1MB chunks keeps the peak flat regardless of file size.
    """
    import shutil

    src = upload_file.file
    try:
        src.seek(0)
    except Exception:
        pass
    with open(dst_path, "wb") as out:
        if max_bytes is None:
            shutil.copyfileobj(src, out, chunk_bytes)
        else:
            written = 0
            while True:
                chunk = src.read(chunk_bytes)
                if not chunk:
                    break
                written += len(chunk)
                if written > max_bytes:
                    raise UploadTooLarge(max_bytes)
                out.write(chunk)
    return os.path.getsize(dst_path)


def _v864_release(tag=None):
    """Return freed heap to the OS so the headroom figure reflects reality
    before we decide whether a ~250MB model load is safe. Mirrors the v701z
    sequencing that transcribe_master_audio's own memory budget assumes has
    already run.

    v872 — one implementation, in mem_guard.trim(). This wrapper stays because
    several call sites already use the v864 name; it now also logs the RSS
    actually returned, which is the number that tells us whether the trim is
    doing anything at all.
    """
    try:
        import mem_guard as _mg872
        _mg872.trim(tag)
        return
    except Exception:
        pass
    # Fallback if mem_guard is somehow unavailable (never on Render).
    try:
        import gc as _gc
        _gc.collect()
    except Exception:
        pass
    try:
        import ctypes as _ct
        _ct.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


# v872 — how much container headroom an export needs before it may START.
# Exports are durable (v850): a run that waits stays queued and fires on the
# next tick. A run that starts into 150MB of headroom takes the whole container
# down with it and loses every finished clip of work — that is what happened to
# job be09f595 on 2026-07-28.
EXPORT_MIN_AVAIL_MB = int(os.environ.get("EXPORT_MIN_AVAIL_MB", "600"))

_last_export_gate_log = 0.0
# Monotonic timestamp of the first tick where the gate held an export back and
# has been holding it ever since; None when the gate is not blocking. The idle
# watchdog reads this: memory we cannot reclaim WITH work waiting is the one
# case where recycling early beats waiting out the uptime floor.
_export_gate_blocked_since = None


def _export_headroom_ok() -> bool:
    """True when there is room to start an export. Logs at most once a minute
    while it is holding runs back, so a stalled queue is visible but the log is
    not flooded at the 2s dispatch tick."""
    global _last_export_gate_log, _export_gate_blocked_since
    try:
        import mem_guard as _mg872
        ok, snap = _mg872.headroom_ok(EXPORT_MIN_AVAIL_MB)
    except Exception:
        return True  # never let the gate itself block exports
    import time as _t
    if ok:
        _export_gate_blocked_since = None
    else:
        if _export_gate_blocked_since is None:
            _export_gate_blocked_since = _t.monotonic()
    if not ok:
        now = _t.monotonic()
        if now - _last_export_gate_log > 60:
            _last_export_gate_log = now
            print(
                f"[Export/v872] HOLD — avail={snap.get('avail_mb')}MB < "
                f"{EXPORT_MIN_AVAIL_MB}MB required (used={snap.get('used_mb')}MB "
                f"of {snap.get('limit_mb')}MB, rss={snap.get('rss_mb')}MB). "
                f"Queued exports stay queued until memory frees up.",
                flush=True,
            )
    return ok


# v872 — idle recycle backstop.
#
# Everything else in v872 makes the process give memory back. This is what
# happens when it STILL doesn't. A gunicorn worker that exits is respawned by
# the arbiter (that is how --max-requests already works), and a fresh worker
# starts at the ~173MB baseline. Firing it ONLY when nothing is running turns
# the old failure mode — "container OOM-killed mid-export, 15 minutes of work
# lost" — into "5 seconds of 502 while idle".
RECYCLE_IDLE_RSS_MB = int(os.environ.get("RECYCLE_IDLE_RSS_MB", "1000"))
RECYCLE_MIN_UPTIME_S = int(os.environ.get("RECYCLE_MIN_UPTIME_S", "900"))
import time as _time_v872
_PROCESS_START_MONO = _time_v872.monotonic()


def _container_is_idle() -> bool:
    """No export running on this container and no video-generation job in
    flight. Image-platform pollers are external processes that retry, so they
    do not hold the recycle back."""
    if _LOCAL_EXPORT_IDS:
        return False
    if _LOCAL_AUTOEDIT_IDS:  # v938 — a render here is worth protecting too
        return False
    try:
        if getattr(worker, "running_jobs", None):
            return False
    except Exception:
        return False  # can't prove idle → not idle
    return True


async def _idle_memory_watchdog():
    """Recycle this worker when it is idle and still fat. Never fires during an
    export or a job — those are the two things worth protecting."""
    import time as _t
    import signal as _sig

    hot_checks = 0
    while True:
        await asyncio.sleep(60)
        try:
            if RECYCLE_IDLE_RSS_MB <= 0:
                continue
            # The uptime floor stops a boot-time blip from restarting the
            # worker. It is waived when the export gate has been holding a run
            # back for 3+ minutes: at that point memory we cannot reclaim is
            # actively blocking work, and a 5-second respawn is the fix.
            blocked_for = (
                _t.monotonic() - _export_gate_blocked_since
                if _export_gate_blocked_since is not None else 0
            )
            if (_t.monotonic() - _PROCESS_START_MONO < RECYCLE_MIN_UPTIME_S
                    and blocked_for < 180):
                continue
            if not _container_is_idle():
                hot_checks = 0
                continue

            import mem_guard as _mg872
            _mg872.trim()
            snap = _mg872.snapshot()
            rss = snap.get("rss_mb") or 0
            if rss < RECYCLE_IDLE_RSS_MB:
                hot_checks = 0
                continue

            # Two consecutive fat-and-idle minutes before acting, so a single
            # sample taken mid-teardown can't trigger a pointless restart.
            hot_checks += 1
            if hot_checks < 2:
                print(
                    f"[Mem/v872] idle but fat: rss={rss}MB >= {RECYCLE_IDLE_RSS_MB}MB "
                    f"(check {hot_checks}/2) — trim did not reclaim it",
                    flush=True,
                )
                continue

            if not _container_is_idle():   # last look before pulling the pin
                hot_checks = 0
                continue

            print(
                f"[Mem/v872] RECYCLING WORKER — idle, rss={rss}MB "
                f"(used={snap.get('used_mb')}MB of {snap.get('limit_mb')}MB) stayed "
                f"above {RECYCLE_IDLE_RSS_MB}MB after a trim. Gunicorn respawns a "
                f"fresh worker; no export or job is running.",
                flush=True,
            )
            os.kill(os.getpid(), _sig.SIGTERM)
            return
        except asyncio.CancelledError:
            raise
        except Exception as _we:
            print(f"[Mem/v872] idle watchdog tick failed (non-fatal): {_we}", flush=True)


async def _export_dispatcher():
    """v855 — THE ONLY PLACE A RUNNER IS EVER SPAWNED.

    Before v855 two places fired runners: the POST (on the request) and the
    sweeper. Neither had a cap. So N queued exports for N different jobs became
    N simultaneous ffmpeg+Whisper runs on a 2 GB / 1 CPU box, and a deploy that
    orphaned N runs made the next container fire all N at boot — OOM, restart,
    fire them again. The frontend's global "Export already in progress" alert
    was the only thing holding that back, which is why the operator could not
    queue a second export at all.

    Now the POST just inserts a queued row and the sweeper just re-queues. This
    loop starts them, oldest first, and never exceeds MAX_CONCURRENT.

    Slots are counted from _LOCAL_EXPORT_IDS — this container's LIVE tasks —
    never from a DB count of state='running'. A row stranded in 'running' by a
    dead container would otherwise hold a slot forever and stall the queue for
    good; rescuing those rows is the sweeper's job, not the counter's.
    """
    while True:
        try:
            free = _eq.slots_free(len(_LOCAL_EXPORT_IDS))
            # v872 — memory admission gate. A free SLOT is not the same thing as
            # free MEMORY: on 2026-07-28 the slot was free, the container had
            # ~130MB of headroom left from the previous run's un-returned heap,
            # and the export started anyway and was OOM-killed at 99%.
            if free > 0 and not _export_headroom_ok():
                free = 0
            if free > 0:
                for _rid in await asyncio.to_thread(_next_queued_export_ids, free):
                    print(f"[Export/v855] DISPATCH run={_rid[:8]} "
                          f"({len(_LOCAL_EXPORT_IDS)}/{_eq.MAX_CONCURRENT} slots in use)",
                          flush=True)
                    _spawn_export_runner(_rid)
        except asyncio.CancelledError:
            raise
        except Exception as _de:
            # Never die: if this loop stops, every export sits queued forever
            # and nothing tells anyone.
            print(f"[Export/v855] dispatcher tick failed (non-fatal): {_de}", flush=True)
        await asyncio.sleep(_eq.DISPATCH_INTERVAL_S)


def _queue_export_run(db, job, settings: "ExportSettings", user_id):
    """QUEUE an export (v850 shape). Factored out of export_final_video so the
    v947 auto-finish trigger and the endpoint share one body. Idempotent: an
    active run is joined, not duplicated. Returns (run, created: bool).

    The join is a plain read, NOT a lock — callers that can race each other
    serialize upstream on the job row (see _maybe_auto_finish_export). This is
    also only half the protection: ACTIVE_STATES covers queued/running, so a
    caller that can re-fire after an export has already finished must not rely
    on the join to deduplicate it.
    """
    from models import ExportRun
    existing = db.query(ExportRun).filter(
        ExportRun.job_id == job.id,
        ExportRun.state.in_(list(_eq.ACTIVE_STATES)),
    ).order_by(ExportRun.created_at.desc()).first()
    if existing:
        print(f"[Export/v850] job={job.id[:8]} already has run={existing.id[:8]} "
              f"({existing.state}); joining it", flush=True)
        return existing, False
    run = ExportRun(
        id=str(uuid.uuid4()),
        job_id=job.id,
        user_id=user_id,
        state=_eq.STATE_QUEUED,
        settings_json=settings.model_dump_json(),
        attempts=0,
        created_at=datetime.utcnow(),
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    print(f"[Export/v850] QUEUED run={run.id[:8]} job={job.id[:8]}", flush=True)
    return run, True


def _claim_export_run(export_id: str):
    """Compare-and-swap claim on an ExportRun. Sync — call via to_thread.

    The whole safety of the sweeper rests here: the sweeper is ALLOWED to be
    wrong about a run being orphaned, because the claim only wins if the row is
    still QUEUED. Two claimants can never both flip the same row to running.

    WHY the predicate is queued-ONLY, and why it must never be loosened:
    nothing in this system ever hands the runner a 'running' row — the POST
    inserts 'queued', the sweeper re-queues to 'queued', and the shutdown
    handover re-queues to 'queued'. So allowing 'running' in the WHERE buys
    nothing and costs everything: a stray second spawn for the same export_id
    would WIN the CAS against an export that is already running and start a
    SECOND 15-minute ffmpeg job on Render's single CPU. queued-only is the
    backstop that makes a double-spawn harmless (the loser gets rowcount=0).

    Returns a plain dict (NOT a detached ORM object) or None if the claim lost.
    """
    from models import get_db
    from sqlalchemy import text as _sql_text

    with get_db() as _db:
        try:
            _now = datetime.utcnow()
            res = _db.execute(
                _sql_text(
                    "UPDATE export_runs SET state='running', attempts=attempts+1, "
                    "started_at=:now, heartbeat_at=:now "
                    "WHERE id=:id AND state=:queued"
                ),
                {"id": export_id, "now": _now, "queued": _eq.STATE_QUEUED},
            )
            if res.rowcount != 1:
                _db.rollback()
                print(f"[Export/v850] claim LOST run={export_id[:8]} "
                      f"(rowcount={res.rowcount}) — already running, claimed or terminal", flush=True)
                return None
            _db.commit()

            from models import ExportRun
            row = _db.query(ExportRun).filter(ExportRun.id == export_id).first()
            if row is None:
                print(f"[Export/v850] claim WON but row vanished run={export_id[:8]}", flush=True)
                return None
            return {
                "id": row.id,
                "job_id": row.job_id,
                "user_id": row.user_id,
                "settings_json": row.settings_json,
                "attempts": row.attempts,
            }
        except Exception as _ce:
            try:
                _db.rollback()
            except Exception:
                pass
            print(f"[Export/v850] claim ERROR run={export_id[:8]}: {_ce}", flush=True)
            return None


def _finish_export_run(export_id: str, state: str, result: dict = None,
                       error: str = None) -> bool:
    """v850 — write an export's terminal state on a FRESH session. Sync — to_thread.

    The runner holds ONE session for the whole 15-minute export. If Postgres
    dropped that connection mid-run, writing the outcome on it fails too and
    the row is stranded in 'running' — the sweeper would then re-run an export
    that actually failed, burning all MAX_ATTEMPTS on a doomed job. A new short
    session is the only way the outcome reliably lands.
    """
    from models import get_db, ExportRun

    with get_db() as _db:
        try:
            row = _db.query(ExportRun).filter(ExportRun.id == export_id).first()
            if row is None:
                print(f"[Export/v850] finish: run={export_id[:8]} vanished", flush=True)
                return False
            row.state = state
            if result is not None:
                row.result_json = json.dumps(result)
            if error is not None:
                row.error = str(error)[:2000]
            row.finished_at = datetime.utcnow()
            _db.commit()
            return True
        except Exception as _fe:
            try:
                _db.rollback()
            except Exception:
                pass
            print(f"[Export/v850] could not write terminal state "
                  f"run={export_id[:8]} state={state}: {_fe}", flush=True)
            return False


async def _export_heartbeat(export_id: str):
    """Tick heartbeat_at every HEARTBEAT_INTERVAL_S while this container owns
    the run. A missed tick is survivable (see export_queue.STALE_AFTER_S); a
    dead loop is not, so no exception is allowed to escape."""
    def _tick():
        from models import get_db, ExportRun
        with get_db() as _db:
            try:
                row = _db.query(ExportRun).filter(
                    ExportRun.id == export_id,
                    ExportRun.state == _eq.STATE_RUNNING,
                ).first()
                if row is not None:
                    row.heartbeat_at = datetime.utcnow()
                    _db.commit()
            except Exception:
                try:
                    _db.rollback()
                except Exception:
                    pass
                raise

    while True:
        try:
            await asyncio.sleep(_eq.HEARTBEAT_INTERVAL_S)
            await asyncio.to_thread(_tick)
        except asyncio.CancelledError:
            raise
        except Exception as _he:
            print(f"[Export/v850] heartbeat error run={export_id[:8]}: {_he}", flush=True)


def _maybe_auto_finish_autoedit(job_id: str):
    """v947 — after a DONE export on an auto_finish job, queue the auto-edit
    with the build's declared settings. This is the SECOND half of the chain
    that _maybe_auto_finish_export starts when the last clip is approved.
    Sync; the runner calls it via to_thread with a FRESH session (the runner's
    own session may well be dead by the time a long export completes).

    Declared autoedit_* fields are passed as an EXPLICIT request, so they beat
    the stored-run hook-layout inheritance; captions/overlay still derive from
    the same spec inside the impl (v944). A double fire is a logged no-op, not
    a duplicate: _queue_autoedit_impl takes the job row FOR UPDATE before its
    can_queue read, so a racing caller blocks and then sees the winner's run.

    Every outcome writes a job LOG as well as printing. Nobody is watching
    stdout on a chain whose whole point is that nobody is watching.
    """
    from models import get_db, Job
    from auto_finish import auto_finish_on
    with get_db() as db:
        job = db.query(Job).filter(Job.id == job_id).first()
        if job is None:
            return
        spec = _job_finishing_spec(job)
        if not auto_finish_on(spec):
            return
        # FAIL CLOSED on a corrupt declaration. Degrading to defaults would
        # render with settings the build never asked for — the exact v944
        # failure class this whole feature exists to kill.
        try:
            req = AutoEditRequest(**(spec.get("autoedit") or {}))
        except Exception as _ve:
            msg = (f"Auto-finish: auto-edit NOT queued — declared autoedit settings "
                   f"failed validation ({str(_ve)[:200]}); re-import the build")
            print(f"[AutoFinish] job={job_id[:8]} {msg}", flush=True)
            try:
                add_job_log(db, job.id, msg, "ERROR", "auto_finish")
            except Exception:
                pass
            return
        try:
            run = _queue_autoedit_impl(db, job, req, job.user_id)
        except HTTPException as e:
            print(f"[AutoFinish] job={job_id[:8]} autoedit not queued: {e.detail}",
                  flush=True)
            try:
                add_job_log(db, job.id,
                            f"Auto-finish: auto-edit not queued — {e.detail}",
                            "WARNING", "auto_finish")
            except Exception as _le:
                print(f"[AutoFinish] job={job_id[:8]} could not log the skip: {_le}",
                      flush=True)
            return
        print(f"[AutoFinish] job={job_id[:8]} export done -> autoedit "
              f"run={run.id[:8]} queued", flush=True)
        # The log write runs AFTER the run row is committed on purpose, and its
        # failure is swallowed: losing the log line beats losing the run (same
        # trade as _maybe_auto_finish_export).
        try:
            add_job_log(db, job.id,
                        f"Auto-finish: export done — auto-edit queued ({run.id[:8]})",
                        "INFO", "auto_finish")
        except Exception as _le:
            print(f"[AutoFinish] job={job_id[:8]} could not log the queue: {_le}",
                  flush=True)


async def _export_runner(export_id: str):
    """Do the export, detached from any HTTP request."""
    import traceback as _tb
    from models import get_db, User as _User   # terminal writes go through
                                               # _finish_export_run's own session

    hb_task = None
    try:
        claim = await asyncio.to_thread(_claim_export_run, export_id)
        if claim is None:
            print(f"[Export/v850] SKIP run={export_id[:8]} — claim not won", flush=True)
            return

        job_id = claim["job_id"]
        print(f"[Export/v850] START run={export_id[:8]} job={job_id[:8]} "
              f"attempt={claim['attempts']}/{_eq.MAX_ATTEMPTS}", flush=True)

        # Set to job_id ONLY on the success path, and acted on AFTER the
        # session below is closed — see the chain block under the with.
        chain_after = None

        # Its OWN long-lived session: the request-scoped one died with the POST
        # that queued this run.
        with get_db() as db:
            try:
                settings = ExportSettings(**json.loads(claim["settings_json"]))
                user = db.query(_User).filter(_User.id == claim["user_id"]).first()
                if user is None:
                    raise RuntimeError(f"user {claim['user_id']} no longer exists")

                hb_task = asyncio.create_task(_export_heartbeat(export_id))

                result = await _do_export_final(job_id, settings, db, user)

                # Terminal write goes on a FRESH session — never on `db`, which
                # has been open for the whole export and may well be dead by now.
                await asyncio.to_thread(
                    _finish_export_run, export_id, _eq.STATE_DONE, result, None
                )
                print(f"[Export/v850] DONE run={export_id[:8]} job={job_id[:8]} "
                      f"→ {result.get('filename')}", flush=True)
                chain_after = job_id
            except Exception as e:
                # _do_export_final was a route handler, so it still raises
                # HTTPException — which subclasses Exception and lands here, but
                # whose str() is useless ("500: ..." at best, "" at worst). The
                # UI shows this text, so take .detail when there is one.
                _err = getattr(e, "detail", None) or str(e) or e.__class__.__name__
                print(f"[Export/v850] FAILED run={export_id[:8]} job={job_id[:8]}: {_err}", flush=True)
                print(f"[Export/v850] traceback: {_tb.format_exc()}", flush=True)
                try:
                    db.rollback()
                except Exception:
                    pass
                # Fresh session again: if the export died BECAUSE the connection
                # dropped, `db` cannot record its own failure.
                await asyncio.to_thread(
                    _finish_export_run, export_id, _eq.STATE_FAILED, None, _err
                )

        # v947 — the declared finish continues by itself. Runs OUTSIDE the
        # with-block on purpose: the chain opens its own session and takes the
        # job row FOR UPDATE, and doing that while the export's long-lived
        # session still held an idle transaction on the same row is a hang
        # waiting to happen. Own try: a chain error must never re-mark the
        # finished export as failed.
        if chain_after:
            try:
                await asyncio.to_thread(_maybe_auto_finish_autoedit, chain_after)
            except Exception as _af:
                print(f"[AutoFinish] job={chain_after[:8]} autoedit chain error: "
                      f"{_af}", flush=True)
                # Best effort, fresh session — the operator's only other view of
                # this failure is a stdout line nobody reads.
                try:
                    with get_db() as _ldb:
                        add_job_log(_ldb, chain_after,
                                    f"Auto-finish: auto-edit chain error — "
                                    f"{str(_af)[:200]}", "ERROR", "auto_finish")
                except Exception:
                    pass
    finally:
        if hb_task is not None:
            hb_task.cancel()
        _LOCAL_EXPORT_IDS.discard(export_id)


def _sweep_stale_exports() -> list:
    """Re-queue every ExportRun a dead container left behind, and RETURN their
    ids for the caller to spawn. Sync — call via to_thread.

    Stale == the owning container stopped heartbeating. That is precisely what
    a Render deploy / OOM / crash looks like from the outside. Runs alive in
    THIS container are skipped by id, so a slow export is never double-started;
    and even if this call is wrong, _claim_export_run's CAS is the real guard.
    """
    from models import get_db, ExportRun

    to_fire = []
    with get_db() as _db:
        try:
            runs = _db.query(ExportRun).filter(
                ExportRun.state.in_(list(_eq.ACTIVE_STATES))
            ).all()
            _now = datetime.utcnow()
            for run in runs:
                if run.id in _LOCAL_EXPORT_IDS:
                    continue  # alive right here
                if not _eq.is_stale(run.state, run.heartbeat_at, _now):
                    continue  # someone is still ticking it
                if _eq.next_state_after_reclaim(run.attempts) == _eq.STATE_FAILED:
                    run.state = _eq.STATE_FAILED
                    run.error = (
                        f"Export was killed {run.attempts} time(s) before it could finish "
                        f"(server restart / out-of-memory). Giving up after "
                        f"{_eq.MAX_ATTEMPTS} attempts — please retry the export."
                    )
                    run.finished_at = _now
                    print(f"[Export/v850] GIVE UP run={run.id[:8]} job={run.job_id[:8]} "
                          f"after {run.attempts} attempt(s)", flush=True)
                    continue
                run.state = _eq.STATE_QUEUED
                run.heartbeat_at = None
                to_fire.append(run.id)
                print(f"[Export/v850] RECLAIM run={run.id[:8]} job={run.job_id[:8]} "
                      f"attempts={run.attempts} — heartbeat stale, re-queueing", flush=True)
            _db.commit()
        except Exception as _se:
            try:
                _db.rollback()
            except Exception:
                pass
            print(f"[Export/v850] sweep error: {_se}", flush=True)
            return 0

    # v854 — RETURN the ids; do NOT spawn here.
    #
    # This function is sync and every caller runs it via asyncio.to_thread, i.e.
    # on a worker thread with NO event loop. Calling _spawn_export_runner here
    # (which calls asyncio.create_task) raised "RuntimeError: no running event
    # loop" on EVERY sweep — and because the id was added to _LOCAL_EXPORT_IDS
    # before create_task blew up, the id stuck there with no task behind it, so
    # every later sweep skipped that run forever. A deploy-orphaned export sat
    # queued with attempts=1 and heartbeat=NULL until someone noticed. Both
    # sweep paths (boot + 60s) were dead from day one; only the POST path
    # worked, because that one runs on the loop.
    #
    # Spawning is now the async caller's job. This function only touches the DB.
    return to_fire


async def _export_sweeper():
    """Every 60s, re-fire orphaned exports.

    The graceful-shutdown hook covers the normal deploy. This loop covers the
    hard kill (OOM / SIGKILL) where that hook never got to run — the row just
    stops heartbeating and nothing else would ever notice.
    """
    while True:
        try:
            await asyncio.sleep(60)
            # v855 — the sweep only RE-QUEUES. The dispatcher starts them, so
            # N orphans can never become N simultaneous ffmpeg runs.
            ids = await asyncio.to_thread(_sweep_stale_exports)
            if ids:
                print(f"[Export/v850] sweeper re-queued {len(ids)} orphaned export(s) "
                      f"— the dispatcher will start them one at a time", flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as _e:
            print(f"[Export/v850] sweeper error: {_e}", flush=True)


def _requeue_local_exports_on_shutdown() -> int:
    """Deploy path: hand this container's in-flight exports to the next one.

    Setting heartbeat_at=NULL makes the row stale IMMEDIATELY, so the next
    container's boot sweep picks it up at once instead of waiting out
    STALE_AFTER_S. Sync — call via to_thread from the shutdown hook.
    """
    from models import get_db, ExportRun

    ids = list(_LOCAL_EXPORT_IDS)
    if not ids:
        return 0
    n = 0
    with get_db() as _db:
        try:
            for run in _db.query(ExportRun).filter(
                ExportRun.id.in_(ids),
                ExportRun.state == _eq.STATE_RUNNING,
            ).all():
                run.state = _eq.STATE_QUEUED
                run.heartbeat_at = None
                n += 1
                print(f"[Export/v850] HANDOVER run={run.id[:8]} job={run.job_id[:8]} "
                      f"— shutting down, re-queued for the next container", flush=True)
            _db.commit()
        except Exception as _qe:
            try:
                _db.rollback()
            except Exception:
                pass
            print(f"[Export/v850] shutdown re-queue error: {_qe}", flush=True)
            return 0
    print(f"[Export/v850] shutdown handed over {n} in-flight export(s)", flush=True)
    return n


# ============ v938 — server-side auto-edit executor ============
#
# Auto-edit used to be queue-ONLY: the server inserted a row and a worker on
# the operator's PC claimed it. A user with no PC setup got a row that sat
# 'queued' for ever and no video. This runs the same pipeline HERE, the same
# way Export Final already runs here, so the feature works with nobody at home.
#
# It deliberately copies the export design above so the two read alike: one
# dispatcher loop is the only place a runner is spawned, the claim takes a row
# lock, the runner opens its OWN session, heartbeats while it works, and writes
# the terminal state on a fresh session.
#
# The caption renderer is left alone on purpose: autoedit_pipeline.caption_engine()
# returns 'libass' (plain ffmpeg) unless AUTOEDIT_CAPTION_ENGINE says otherwise,
# and that is the only engine this 2GB box can run. Never set that variable here.

# Off switch. Default ON; set to 0/false/no/off in the Render env to stop the
# server rendering without waiting for a deploy. The worker path is untouched
# by it — turning this off just returns auto-edit to queue-only.
def _autoedit_server_enabled() -> bool:
    # v938.3 — DEFAULT ON. (A v938.3 comment here briefly claimed renders did
    # not fit in 2GB and turned this off; that was a misdiagnosis, corrected
    # below, because getting it wrong in either direction is expensive.)
    #
    # What actually happened on 2026-08-21: a run burned all three attempts
    # while the container restarted at 17:08:51, 17:31:29 and 17:43:00 UTC.
    # Those three times match three pushes to main (19:04:54, 19:28:48 and
    # 19:40-19:42 local, UTC+2) — a deploy restarts the container by design.
    # It was eight deploys during an active render, not memory.
    #
    # The memory numbers say the same: peak 1216MB of 2048MB (59%), 880-990MB
    # free throughout. The "avail=488MB < 600MB" line that misled the first
    # reading is the EXPORT gate's own conservative threshold, not the
    # container limit. A full render completed in 12 minutes.
    #
    # Real limitation to keep in mind: a deploy DOES kill an in-flight render.
    # The run is reclaimed after the 5-minute stale window and retried, but it
    # starts over and only gets MAX_ATTEMPTS tries — so avoid deploying while
    # a render is running, and expect restarts to cost a render during a
    # deploy-heavy session.
    return (os.environ.get("AUTOEDIT_SERVER_EXECUTOR") or "1").strip().lower() \
        not in ("0", "false", "no", "off")


AUTOEDIT_DISPATCH_INTERVAL_S = int(os.environ.get("AUTOEDIT_DISPATCH_INTERVAL_S", "20"))
# GRACE PERIOD — why the server waits before taking a run.
#
# The operator's PC is far faster than this box (GPU Whisper, pycaps captions,
# spare RAM), so when a local worker is up it SHOULD win every race. It polls
# every ~15s. If the server claimed the instant a row appeared, it would steal
# work from the faster machine roughly half the time. Holding queued rows for
# 90s means a live local worker always gets there first, and the server only
# picks up what nobody claimed.
AUTOEDIT_SERVER_GRACE_S = int(os.environ.get("AUTOEDIT_SERVER_GRACE_S", "90"))
# Same admission gate exports use, and the same default: a render loads a
# Whisper model and runs ffmpeg, so starting into 150MB of headroom takes the
# whole container down.
AUTOEDIT_MIN_AVAIL_MB = int(os.environ.get("AUTOEDIT_MIN_AVAIL_MB", str(EXPORT_MIN_AVAIL_MB)))
# One at a time, always. Two ffmpeg+Whisper passes on 1 CPU / 2GB is an OOM.
AUTOEDIT_MAX_CONCURRENT = 1
# autoedit_queue.STALE_AFTER is 5 minutes and a caption pass can run for
# minutes with no stage change, so beat well inside that window.
AUTOEDIT_HEARTBEAT_S = 45

# Runs THIS container is actively rendering. Same job as _LOCAL_EXPORT_IDS.
_LOCAL_AUTOEDIT_IDS: set = set()
_AUTOEDIT_TASKS: set = set()  # strong refs; asyncio only holds weak ones
_last_autoedit_gate_log = 0.0


def _autoedit_headroom_ok() -> bool:
    """True when there is room to start a render. Logs at most once a minute
    while it holds runs back, so a stalled queue is visible without flooding
    the log at every dispatch tick.

    Deliberately does NOT touch _export_gate_blocked_since: that flag exists to
    let the idle watchdog recycle the worker when an EXPORT is starved, and a
    starved auto-edit is not worth a restart.
    """
    global _last_autoedit_gate_log
    try:
        import mem_guard as _mg
        ok, snap = _mg.headroom_ok(AUTOEDIT_MIN_AVAIL_MB)
    except Exception:
        return True  # never let the gate itself block work
    if not ok:
        import time as _t
        now = _t.monotonic()
        if now - _last_autoedit_gate_log > 60:
            _last_autoedit_gate_log = now
            print(f"[AutoEdit/server] HOLD — avail={snap.get('avail_mb')}MB < "
                  f"{AUTOEDIT_MIN_AVAIL_MB}MB required (used={snap.get('used_mb')}MB "
                  f"of {snap.get('limit_mb')}MB). Queued auto-edits stay queued.",
                  flush=True)
    return ok


def _autoedit_server_token(db, user_id: str):
    """A worker token for this user, so the pipeline can fetch the job's files.

    autoedit_pipeline downloads the export + b-roll over the public API, and it
    reads its token from KAVENO_API_TOKEN. On a PC that token is set up by hand;
    here nobody is around to do it, so reuse the user's active worker token and
    mint one if they have none — the same get-or-create every worker-setup
    endpoint already does. The token is scoped to that one user, and the server
    is only using it to read files it already stores.
    """
    from models import UserWorkerToken
    tok = db.query(UserWorkerToken).filter(
        UserWorkerToken.user_id == user_id,
        UserWorkerToken.is_active == True,  # noqa: E712
    ).first()
    if tok:
        return tok.id
    tok = UserWorkerToken(
        id=secrets.token_urlsafe(48),
        user_id=user_id,
        name=f"Worker-{datetime.utcnow().strftime('%Y%m%d-%H%M')}",
    )
    db.add(tok)
    db.commit()
    print(f"[AutoEdit/server] minted a worker token for user {str(user_id)[:8]} "
          f"— the render needs one to read the job's files", flush=True)
    return tok.id


def _claim_autoedit_for_server():
    """Take the oldest runnable AutoEditRun for this container. Sync — to_thread.

    Returns a plain dict (never a detached ORM row) or None when there is
    nothing to take.
    """
    from models import get_db, AutoEditRun, Job
    from autoedit_queue import is_claimable

    now = datetime.utcnow()
    grace_cutoff = now - timedelta(seconds=AUTOEDIT_SERVER_GRACE_S)
    with get_db() as _db:
        try:
            # claimed/running are in the filter ON PURPOSE — that is the ONLY
            # way a row abandoned by a hard kill (OOM / SIGKILL, where the
            # shutdown hook never ran) ever gets picked up again. is_claimable
            # below decides whether it has really been abandoned; there is no
            # separate sweeper because this loop already reaches those rows.
            #
            # skip_locked mirrors /api/autoedit/claim: without the row lock two
            # claimants reading at the same instant both pass is_claimable and
            # both commit, and one job gets rendered twice. Postgres honours it;
            # SQLite (tests) has no row locks and silently ignores it.
            rows = _db.query(AutoEditRun).filter(
                AutoEditRun.state.in_(["queued", "claimed", "running"])
            ).order_by(AutoEditRun.created_at.asc()).with_for_update(
                skip_locked=True).all()
            for run in rows:
                if run.id in _LOCAL_AUTOEDIT_IDS:
                    continue  # already rendering right here
                if not is_claimable(run.state, run.heartbeat_at, now):
                    continue
                # The grace period guards a FRESH queue entry ONLY — note the
                # state test. A claimed/running row reaches this line only after
                # STALE_AFTER (5 min) of silence, which already means whoever
                # held it is gone; making it wait out the grace window too would
                # be the "stuck for ever" bug this feature has hit twice.
                if run.state == "queued" and (run.created_at or now) > grace_cutoff:
                    continue
                if not run.user_id:
                    continue  # no user, no token, no download — leave it alone
                run.state, run.claimed_by, run.heartbeat_at = "claimed", "server", now
                # A stale reclaim counts as an attempt, same as the worker path,
                # so a run that keeps dying burns the MAX_ATTEMPTS budget.
                run.attempts += 1
                _db.commit()
                print(f"[AutoEdit/server] TEMP claimed {run.id[:8]} job={run.job_id[:8]} "
                      f"template={run.template} attempt={run.attempts}", flush=True)
                job = _db.query(Job).filter(Job.id == run.job_id).first()
                claim = {
                    "id": run.id, "job_id": run.job_id, "user_id": run.user_id,
                    "template": run.template, "placement": run.placement,
                    "offset": run.offset, "repair_json": run.repair_json,
                    "attempts": run.attempts,
                    # Where this job's files already sit on THIS disk. The
                    # render copies them from here instead of downloading them
                    # back through our own public URL.
                    "output_dir": job.output_dir if job else None,
                }
                # The token lookup comes AFTER the claim commit on purpose:
                # minting one commits, and committing while we still held the
                # FOR UPDATE locks would drop them before the claim was written.
                claim["api_token"] = _autoedit_server_token(_db, run.user_id)
                return claim
            _db.rollback()  # release the FOR UPDATE locks we did not use
            return None
        except Exception as _ce:
            try:
                _db.rollback()
            except Exception:
                pass
            print(f"[AutoEdit/server] claim error: {_ce}", flush=True)
            return None


def _autoedit_touch(autoedit_id: str, stage: str = None) -> bool:
    """Heartbeat (and optionally the stage label) on a FRESH short session.

    Filtered on claimed/running so a cancelled or already-failed row is never
    dragged back to life by a late tick from a render we lost.
    """
    from models import get_db, AutoEditRun

    with get_db() as _db:
        try:
            row = _db.query(AutoEditRun).filter(
                AutoEditRun.id == autoedit_id,
                AutoEditRun.state.in_(["claimed", "running"]),
            ).first()
            if row is None:
                return False
            row.state = "running"
            row.heartbeat_at = datetime.utcnow()
            if stage:
                row.stage = stage
            _db.commit()
            return True
        except Exception as _te:
            try:
                _db.rollback()
            except Exception:
                pass
            print(f"[AutoEdit/server] heartbeat write failed "
                  f"run={autoedit_id[:8]}: {_te}", flush=True)
            return False


async def _autoedit_heartbeat(autoedit_id: str):
    """Keep the row fresh while the render blocks a worker thread. A missed
    tick is survivable (STALE_AFTER is 5 min); a dead loop is not, so nothing
    is allowed to escape."""
    while True:
        try:
            await asyncio.sleep(AUTOEDIT_HEARTBEAT_S)
            await asyncio.to_thread(_autoedit_touch, autoedit_id, None)
        except asyncio.CancelledError:
            raise
        except Exception as _he:
            print(f"[AutoEdit/server] heartbeat error run={autoedit_id[:8]}: {_he}",
                  flush=True)


def _autoedit_normalize_qc(qc, source: str) -> dict:
    """Force a usable quality verdict. A render that reports nothing readable
    is 'needs a human', never silently 'READY'."""
    if not isinstance(qc, dict) or qc.get("verdict") not in ("READY", "NEEDS_MANUAL_EDIT"):
        return {
            "schema_version": 1,
            "verdict": "NEEDS_MANUAL_EDIT",
            "reasons": [f"{source} did not provide a valid quality verdict"],
            "checks": [],
        }
    return qc


async def _autoedit_store_result(db, run, qc: dict, write_tmp) -> dict:
    """THE one place a finished auto-edit becomes a stored output.

    Both finishers go through here — the worker's /complete upload and the
    server's own render — so the filename scheme, the R2 key, the qc columns
    and the terminal state can never drift apart. `write_tmp` is an async
    callable that fills the temp path it is handed; that is the only part the
    two paths do differently (read an upload stream vs copy a local file).
    """
    from datetime import datetime as _dt
    from models import Job

    job = db.query(Job).filter(Job.id == run.job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="job for this run no longer exists")

    # v938.2 — the name carries a timestamp so the operator can see which
    # auto-edit is the newest. Without it every result sorted under an empty
    # key and three files sat in the list with nothing to tell them apart.
    # The YYYYMMDD_HHMMSS shape is the one the exports list already sorts on.
    _stamp = (run.created_at or datetime.utcnow()).strftime("%Y%m%d_%H%M%S")
    fn = f"autoedit_{run.job_id[:8]}_{run.template}_{_stamp}_{run.id[:6]}.mp4"
    output_dir = Path(job.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    dest = output_dir / fn

    # Write under a temp name, rename only once the file is closed. Writing
    # straight to `dest` would leave a truncated mp4 under the FINAL name if
    # anything went wrong mid-write, and the outputs list would offer that
    # half-file as a finished video.
    tmp = output_dir / f".{fn}.part"
    try:
        await write_tmp(tmp)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    tmp.replace(dest)

    try:  # R2 so it survives a redeploy — same key scheme the exports use.
        # to_thread: upload_file is a synchronous multi-second network call, and
        # this runs on the event loop of a 1-CPU box — inline it would stall
        # every other request for the whole upload.
        from backends.storage import is_storage_configured, get_storage
        if is_storage_configured():
            await asyncio.to_thread(get_storage().upload_file, str(dest),
                                    f"jobs/{run.job_id}/outputs/{fn}")
    except Exception as e:
        print(f"[AutoEdit] R2 upload failed (non-fatal): {e}", flush=True)

    run.state, run.result_filename, run.finished_at = "done", fn, _dt.utcnow()
    run.qc_status = qc["verdict"]
    run.qc_report_json = json.dumps(qc, ensure_ascii=False)
    db.commit()
    print(f"[AutoEdit/v937 TEMP] done {run.id} verdict={qc['verdict']} -> {fn} "
          f"({dest.stat().st_size / 1e6:.1f} MB)", flush=True)
    return {"ok": True, "filename": fn,
            "qc_status": qc["verdict"], "qc_report": qc,
            "download_url": f"/api/jobs/{run.job_id}/outputs/{fn}"}


def _autoedit_apply_failure(run, error: str) -> str:
    """Shared retry decision: back to queued until MAX_ATTEMPTS is spent.
    Mutates the row; the caller commits."""
    from datetime import datetime as _dt
    from autoedit_queue import next_state_on_fail

    run.state = next_state_on_fail(run.attempts)
    run.error = (error or "")[:2000]
    if run.state == "failed":
        run.finished_at = _dt.utcnow()
    return run.state


def _fail_autoedit_run(autoedit_id: str, error: str):
    """Record a server-side failure on a FRESH session. Sync — to_thread.

    Fresh because the render holds its session for minutes: if the run died
    BECAUSE that connection dropped, the old session cannot record its own
    failure and the row would be stranded in 'running'.
    """
    from models import get_db, AutoEditRun

    with get_db() as _db:
        try:
            row = _db.query(AutoEditRun).filter(AutoEditRun.id == autoedit_id).first()
            if row is None:
                return None
            state = _autoedit_apply_failure(row, error)
            _db.commit()
            print(f"[AutoEdit/server] TEMP fail {autoedit_id[:8]} "
                  f"attempt={row.attempts} -> {state}: {str(error)[:160]}", flush=True)
            return state
        except Exception as _fe:
            try:
                _db.rollback()
            except Exception:
                pass
            print(f"[AutoEdit/server] could not write failure "
                  f"run={autoedit_id[:8]}: {_fe}", flush=True)
            return None


def _autoedit_work_dir(job_id: str) -> Path:
    """Scratch space for one render — on the PERSISTENT DISK when there is one.

    v938.8 — this used to be the system temp dir, which a deploy wipes. The
    pipeline caches every stage in here (the downloaded source, scan.json, the
    layout, the cleaned audio, the composed video, the transcript), so putting
    it on the disk that survives a restart turns a redeploy from "redo all 22
    minutes" into "carry on from the last finished stage". That, plus the
    shutdown handover, is what lets a render survive a deploy the way an
    export does.

    Falls back to temp when no persistent disk is mounted (local dev), where
    losing the cache costs nothing.
    """
    # v938.25 — SAY WHY when the persistent disk is not used.
    #
    # This fell back to /tmp in production and nobody could see it: the
    # `except Exception: pass` swallowed the reason, and /tmp is wiped on every
    # container restart, so a redeploy mid-render threw away every cached stage
    # and started the whole pass again. Combined with three deploys during one
    # render that is how a job reached 80 minutes still on an early stage while
    # the queue said attempt=1 (the shutdown handover refunds the attempt on
    # purpose, so the counter does not reveal it either).
    #
    # A silent fallback that costs 20 minutes per restart has to be loud.
    why = ""
    try:
        from config import config
        out = getattr(config, "outputs_dir", None)
        if not out:
            why = "config.outputs_dir is unset"
        else:
            root = Path(out).parent
            if root.exists():
                return root / "autoedit_work" / job_id
            why = f"{root} does not exist"
    except Exception as exc:
        why = f"{type(exc).__name__}: {exc}"
    import tempfile
    tmp = Path(tempfile.gettempdir()) / "autoedit" / job_id
    print(f"[AutoEdit/server] WORK DIR falling back to {tmp} — {why}. "
          f"This is wiped on restart, so a deploy mid-render restarts the whole pass.",
          flush=True)
    return tmp


AUTOEDIT_WORK_KEEP_HOURS = int(os.environ.get("AUTOEDIT_WORK_KEEP_HOURS", "48"))


def _sweep_old_autoedit_work(keep_hours: int = None) -> int:
    """Delete render scratch nobody is coming back for.

    Only touches directories whose newest file is older than the window, so a
    render that is mid-flight or was just handed over by a deploy is never
    swept out from under itself.
    """
    import shutil as _sh, time as _t
    keep = (keep_hours if keep_hours is not None else AUTOEDIT_WORK_KEEP_HOURS) * 3600
    root = _autoedit_work_dir("_probe_").parent
    if not root.exists():
        return 0
    now, freed, n = _t.time(), 0, 0
    for d in root.iterdir():
        if not d.is_dir():
            continue
        try:
            files = [f for f in d.rglob("*") if f.is_file()]
            if not files:
                newest, size = d.stat().st_mtime, 0
            else:
                newest = max(f.stat().st_mtime for f in files)
                size = sum(f.stat().st_size for f in files)
            if now - newest > keep:
                _sh.rmtree(d, ignore_errors=True)
                freed += size
                n += 1
        except Exception as e:
            print(f"[AutoEdit/server] sweep skipped {d.name}: {e}", flush=True)
    if n:
        print(f"[AutoEdit/server] swept {n} stale work dir(s), freed {freed/1e6:.0f}MB", flush=True)
    return n


def _run_autoedit_blocking(claim: dict, work: Path) -> Path:
    """The render itself. Sync and slow — always call via asyncio.to_thread."""
    from autoedit_pipeline import run_autoedit

    work.mkdir(parents=True, exist_ok=True)
    out = work / f"autoedit_{claim['id'][:6]}.mp4"
    try:
        repairs = json.loads(claim.get("repair_json") or "{}")
    except (TypeError, ValueError):
        repairs = {}

    # Two environment hand-offs to the pipeline, both put back below whatever
    # happens. Only one render runs per container (AUTOEDIT_MAX_CONCURRENT), so
    # setting process-wide environment around the call is safe.
    #
    # KAVENO_API_TOKEN — the metadata calls (export-status, list-outputs) still
    # go over the API, so the token is still needed.
    #
    # AUTOEDIT_LOCAL_OUTPUTS — the job's export is already on this disk, so
    # autoedit_pipeline.download() copies it instead of pulling ~150MB back
    # through our own public URL. Set ONLY when the directory is really here: a
    # job whose files live only in R2 must fall through to the HTTP download,
    # and the copy must never be aimed at a directory that is not there. In that
    # case the variable is REMOVED rather than left alone, so one job's outputs
    # directory can never leak into the next render.
    before = {k: os.environ.get(k) for k in
              ("KAVENO_API_TOKEN", "AUTOEDIT_LOCAL_OUTPUTS")}
    out_dir = claim.get("output_dir")
    local_outputs = str(out_dir) if out_dir and Path(out_dir).is_dir() else None
    if claim.get("api_token"):
        os.environ["KAVENO_API_TOKEN"] = claim["api_token"]
    if local_outputs:
        os.environ["AUTOEDIT_LOCAL_OUTPUTS"] = local_outputs
        print(f"[AutoEdit/server] local outputs: {local_outputs} — the job's "
              f"files are copied from disk, not downloaded", flush=True)
    else:
        os.environ.pop("AUTOEDIT_LOCAL_OUTPUTS", None)
        print(f"[AutoEdit/server] no local outputs dir on this box "
              f"({out_dir!r}) — falling back to the HTTP download", flush=True)
    try:
        return run_autoedit(
            claim["job_id"], work, out,
            template=claim["template"], placement=claim["placement"],
            offset=claim.get("offset"),
            progress=lambda stage: _autoedit_touch(claim["id"], stage),
            repairs=repairs or None,
        )
    finally:
        for _k, _v in before.items():
            if _v is None:
                os.environ.pop(_k, None)
            else:
                os.environ[_k] = _v


async def _finish_autoedit_from_path(claim: dict, out: Path, work: Path) -> dict:
    """Store a server-rendered result through the same helper /complete uses."""
    from models import get_db, AutoEditRun

    qc_raw = {}
    report = work / "qc_report.json"
    if report.exists():
        try:
            qc_raw = json.loads(report.read_text(encoding="utf-8"))
        except (OSError, ValueError) as _qe:
            print(f"[AutoEdit/server] unreadable qc_report.json: {_qe}", flush=True)
    qc = _autoedit_normalize_qc(qc_raw, "The server render")

    async def _write(tmp: Path):
        # copyfile, not rename: the work dir is in the system temp area and the
        # outputs dir may well be a different mount, where rename fails.
        await asyncio.to_thread(shutil.copyfile, str(out), str(tmp))

    with get_db() as db:
        run = db.query(AutoEditRun).filter(AutoEditRun.id == claim["id"]).first()
        if run is None:
            raise RuntimeError(f"auto-edit run {claim['id'][:8]} vanished mid-render")
        return await _autoedit_store_result(db, run, qc, _write)


async def _autoedit_runner(claim: dict):
    """Do one auto-edit, detached from any HTTP request."""
    import traceback as _tb

    autoedit_id = claim["id"]
    work = _autoedit_work_dir(claim["job_id"])
    hb_task = None
    ok = False
    try:
        print(f"[AutoEdit/server] START run={autoedit_id[:8]} job={claim['job_id'][:8]} "
              f"attempt={claim['attempts']} work={work}", flush=True)
        hb_task = asyncio.create_task(_autoedit_heartbeat(autoedit_id))
        # to_thread: _autoedit_touch is a blocking DB write and this is the
        # event loop.
        await asyncio.to_thread(_autoedit_touch, autoedit_id, "download")
        out = await asyncio.to_thread(_run_autoedit_blocking, claim, work)
        res = await _finish_autoedit_from_path(claim, out, work)
        ok = True
        print(f"[AutoEdit/server] DONE run={autoedit_id[:8]} -> {res.get('filename')} "
              f"({res.get('qc_status')})", flush=True)
    except asyncio.CancelledError:
        # Shutdown. Do NOT burn an attempt on it — the shutdown hook re-queues
        # this row so the next container picks it straight up.
        raise
    except BaseException as e:
        # run_autoedit raises AutoEditError (a RuntimeError), but the store step
        # can still raise HTTPException, whose str() is useless. Take .detail.
        _err = getattr(e, "detail", None) or str(e) or e.__class__.__name__
        print(f"[AutoEdit/server] FAILED run={autoedit_id[:8]}: {_err}", flush=True)
        print(f"[AutoEdit/server] traceback: {_tb.format_exc()}", flush=True)
        await asyncio.to_thread(_fail_autoedit_run, autoedit_id, str(_err))
    finally:
        if hb_task is not None:
            hb_task.cancel()
        _LOCAL_AUTOEDIT_IDS.discard(autoedit_id)
        if ok:
            # A finished render leaves a ~150MB source video plus intermediates
            # behind, and this box has very little disk. Failures KEEP the dir
            # so it can be looked at — the path is logged above.
            try:
                shutil.rmtree(work, ignore_errors=True)
            except Exception as _ce:
                print(f"[AutoEdit/server] could not clean {work}: {_ce}", flush=True)
        else:
            print(f"[AutoEdit/server] kept work dir for diagnosis: {work}", flush=True)


def _spawn_autoedit_runner(claim: dict) -> None:
    """Fire the runner detached from the dispatcher tick.

    Registers the id SYNCHRONOUSLY, before the task is even scheduled, so a
    later tick landing in the gap cannot start a second render of the same run.
    A failure to create the task unregisters it again — a leaked id would make
    the run unclaimable for the life of the container.
    """
    autoedit_id = claim["id"]
    _LOCAL_AUTOEDIT_IDS.add(autoedit_id)
    try:
        task = asyncio.create_task(_autoedit_runner(claim))
    except RuntimeError as _cte:
        _LOCAL_AUTOEDIT_IDS.discard(autoedit_id)
        print(f"[AutoEdit/server] CANNOT SPAWN run={autoedit_id[:8]} — {_cte}", flush=True)
        raise

    _AUTOEDIT_TASKS.add(task)

    def _on_done(_t: "asyncio.Task") -> None:
        # Belt to the runner's own finally: a coroutine that dies BEFORE its try
        # block never runs that finally, and the id would leak.
        _AUTOEDIT_TASKS.discard(_t)
        _LOCAL_AUTOEDIT_IDS.discard(autoedit_id)
        if _t.cancelled():
            return
        _exc = _t.exception()
        if _exc is not None:
            print(f"[AutoEdit/server] runner task DIED run={autoedit_id[:8]}: "
                  f"{type(_exc).__name__}: {_exc}", flush=True)

    task.add_done_callback(_on_done)


async def _autoedit_dispatch_tick() -> bool:
    """One pass of the dispatcher. Split out so a test can drive it directly.
    Returns True when it started a render."""
    if not _autoedit_server_enabled():
        return False
    if len(_LOCAL_AUTOEDIT_IDS) >= AUTOEDIT_MAX_CONCURRENT:
        return False
    # A free slot is not the same thing as free memory.
    if not _autoedit_headroom_ok():
        return False
    claim = await asyncio.to_thread(_claim_autoedit_for_server)
    if claim is None:
        return False
    _spawn_autoedit_runner(claim)
    return True


async def _autoedit_dispatcher():
    """THE ONLY PLACE a server-side auto-edit runner is spawned."""
    while True:
        try:
            await _autoedit_dispatch_tick()
        except asyncio.CancelledError:
            raise
        except Exception as _de:
            # Never die: if this loop stops, every auto-edit sits queued for
            # ever and nothing says why.
            print(f"[AutoEdit/server] dispatcher tick failed (non-fatal): {_de}",
                  flush=True)
        await asyncio.sleep(AUTOEDIT_DISPATCH_INTERVAL_S)


def _requeue_local_autoedits_on_shutdown() -> int:
    """Deploy path: hand this container's in-flight renders back to the queue.

    heartbeat_at=NULL makes the row claimable at once, so the next container
    (or the local worker) picks it up instead of waiting out the 5-minute stale
    window. Sync — call via to_thread from the shutdown hook.
    """
    from models import get_db, AutoEditRun

    ids = list(_LOCAL_AUTOEDIT_IDS)
    if not ids:
        return 0
    n = 0
    with get_db() as _db:
        try:
            for run in _db.query(AutoEditRun).filter(
                AutoEditRun.id.in_(ids),
                AutoEditRun.state.in_(["claimed", "running"]),
            ).all():
                run.state = "queued"
                run.heartbeat_at = None
                run.stage = None
                # v938.8 — a deploy is not a failure, so it must not spend one
                # of the three attempts. Without this, three deploys during a
                # long render exhaust the retry budget and the run is marked
                # failed even though nothing ever went wrong with it.
                run.attempts = max(0, (run.attempts or 1) - 1)
                n += 1
                print(f"[AutoEdit/server] HANDOVER run={run.id[:8]} job={run.job_id[:8]} "
                      f"— shutting down, re-queued (attempt refunded, now {run.attempts})",
                      flush=True)
            _db.commit()
        except Exception as _qe:
            try:
                _db.rollback()
            except Exception:
                pass
            print(f"[AutoEdit/server] shutdown re-queue error: {_qe}", flush=True)
            return 0
    return n


# =============================================================================
# v943.1 — SOURCE-ORIGINAL AUDIO on a charswap clip
# =============================================================================
# A charswap RENDER is silent by contract: Flow only accepts a muted upload and
# charswap_prepare_source strips the track on the way in. So the source video's
# own audio — the thing that made the source worth swapping in the first place —
# is thrown away before the render, and nothing downstream can put it back.
#
# v943.1 puts it back at EXPORT time. A scene declaring `- **audio:**
# source-original` gets the stored source's audio laid over its segment of the
# final cut. Done per clip, on the downloaded render file, BEFORE the trim /
# VAD / concat pipeline runs — that is the only place a clip's segment is still
# addressable as its own file. After the concat the segment boundaries have
# moved (VAD removes silence, the speed pass rescales) and there is nothing
# left to line the audio up against.
#
# Nothing here may fail the export. Missing source, no audio stream in it,
# ffmpeg unhappy — every one of those leaves the clip exactly as silent as it is
# today and the rest of the video ships.


def charswap_export_audio_key(clip, owner_user_id) -> Optional[str]:
    """The R2 key whose audio belongs over this clip's segment, or None.

    THE decision, kept pure so it can be tested without R2, ffmpeg or a
    database: FOUR facts have to line up — the clip is a charswap render, it
    asked for source audio, it actually has a stored source to take it from,
    and that source belongs to the job's own user. A clip that asked for it
    but carries no source key is not an error here; it just has nothing to
    mux, which is the same silent outcome.

    `owner_user_id` is REQUIRED, and it is the job's user id, never anything
    from the clip row. The export read used to trust the stored key outright:
    the worker download route is owner-scoped, but this one was not, so a key
    written under another account's prefix would have been fetched here. The
    ownership check happens in this pure function precisely so it lands BEFORE
    any storage read.

    Takes a Clip row or a plain dict, so a test does not need the ORM.
    """
    def _field(name):
        if isinstance(clip, dict):
            return clip.get(name)
        return getattr(clip, name, None)

    if str(_field("render_method") or "").strip().lower() != "charswap":
        return None
    if str(_field("swap_audio") or "").strip().lower() != "source-original":
        return None
    key = str(_field("swap_source_r2_key") or "").strip()
    if not key:
        return None
    if owner_user_id is None or not _v943_swap_source_owned_by(key, owner_user_id):
        print(f"[v943.1] REFUSED: source key {key!r} is not under "
              f"swap-sources/{owner_user_id}/ — clip stays silent", flush=True)
        return None
    return key


def _v943_1_has_audio_stream(path) -> bool:
    """True when ffprobe finds an audio stream in the file."""
    info = _v943_probe_source(path)
    return any(
        (s.get("codec_type") or "") == "audio"
        for s in (info.get("streams") or [])
    )


def _v943_1_render_duration(path) -> float:
    """The render's own duration in seconds, or 0.0 when it cannot be read."""
    try:
        from video_processor import get_duration
        return float(get_duration(_v943_probe_source(path)) or 0.0)
    except Exception as e:
        print(f"[v943.1] could not probe render duration for "
              f"{os.path.basename(str(path))}: {e}", flush=True)
        return 0.0


# v952 — how long the source audio fades at the very end of a clip. Long enough
# to read as an ending rather than a cut, short enough not to eat a beat of the
# music. Capped at a quarter of the clip so a very short segment still fades.
RC_AUDIO_FADE_S = 0.35


def _v943_1_mux_argv(render_path, source_path, out_path,
                     render_duration=None) -> list:
    """The ffmpeg call that lays the source's audio over the render.

    The video stream is COPIED, never re-encoded — this clip has already been
    rendered once and a second encode would cost quality for nothing. Only the
    audio is transcoded, to the aac the concat step expects.

    THE VIDEO SETS THE LENGTH. The first version used `-shortest` alone, which
    cuts to whichever input is shorter — so a source audio one second short of
    the render silently chopped a second off the PICTURE. The second padded the
    audio with silence (`apad`) and cut the output at the render's own duration
    (`-t`), which kept every frame but left the tail SILENT.

    v952 — that silent tail is what the operator heard: "the music stopping
    before the end of the video". It is structural, not occasional. Veo returns a
    FIXED ~7.7s container whatever the source length, so a 6s source can never
    fill it. Measured on job bb159509: source audio 6.000s, the export then trims
    7 frames (0.292s) off the head, so sound ended at 5.769s of a 7.479s cut —
    1.70s of dead air on every clip in the lane.

    So the audio now LOOPS to fill the render (`-stream_loop -1` on the audio
    input) instead of padding with silence, and the last RC_AUDIO_FADE_S are
    faded out so the loop seam and the ending both land clean. `apad` stays after
    the loop as the belt for a source ffmpeg refuses to loop; `-shortest` still
    guards the un-probed case, and it terminates because the VIDEO input is
    finite even when the audio input is endless.

    The caveat, stated because it is real: on a source whose audio is SPEECH
    rather than music, looping repeats the last words instead of going quiet.
    This path is the charswap `audio: source-original` lane, which is the
    music-bed case by construction (its scenes are `speaker: silent`), and the
    fade covers a short repeat. A speech source that must not loop should ask for
    `audio: none` and take a voiceover.
    """
    try:
        from video_processor import FFMPEG_BIN as _ffmpeg
    except Exception:
        _ffmpeg = "ffmpeg"
    try:
        _dur = float(render_duration or 0.0)
    except (TypeError, ValueError):
        _dur = 0.0

    _af = "apad"
    if _dur > 0:
        fade = min(RC_AUDIO_FADE_S, _dur / 4.0)
        _af = f"apad,afade=t=out:st={max(0.0, _dur - fade):.6f}:d={fade:.6f}"

    argv = [
        _ffmpeg, "-y", "-loglevel", "error", "-nostats",
        "-i", str(render_path),
        # v952 — loop the AUDIO input so a short source fills the render instead
        # of ending in silence. Applies to the input that follows it only.
        "-stream_loop", "-1",
        "-i", str(source_path),
        "-map", "0:v:0", "-map", "1:a:0",
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", "192k", "-ar", "48000", "-ac", "2",
        "-af", _af,
    ]
    if _dur > 0:
        argv += ["-t", f"{_dur:.6f}"]
    else:
        argv += ["-shortest"]
    argv += ["-movflags", "+faststart", str(out_path)]
    return argv


def _v943_1_apply_source_audio(clips, rows, owner_user_id, export_dir) -> int:
    """Mux source audio onto every downloaded clip that asked for it.

    `clips` are the Clip rows; `rows` are the per-clip dicts the download pass
    produced (they carry `_clip_db_id` and the local `path`). Returns how many
    segments got audio. Never raises.

    `owner_user_id` is the JOB's user id. Every key is checked against
    swap-sources/{owner_user_id}/ inside charswap_export_audio_key, so a
    foreign key never reaches storage.download_file at all.

    `export_dir` is a temp directory that belongs to THIS export run, and the
    muxed file is written there — never over the canonical clip. The first
    version moved the muxed file onto `output_dir / clip.output_filename`,
    which is the same path the per-clip output endpoint serves under an
    immutable URL: one export silently changed what a clip URL returned and
    poisoned every later export on that instance. Now only this run's row
    `path` points at the copy; the canonical file stays byte-identical and the
    copy dies with the export.
    """
    import subprocess as _sp
    import tempfile as _tf

    wanted = {}
    for c in clips:
        key = charswap_export_audio_key(c, owner_user_id)
        if key:
            wanted[c.id] = key
    if not wanted:
        return 0

    by_id = {}
    for r in rows:
        if isinstance(r, dict) and r.get("_clip_db_id") is not None:
            by_id[r["_clip_db_id"]] = r

    try:
        from backends.storage import is_storage_configured, get_storage
        storage = get_storage() if is_storage_configured() else None
    except Exception as e:
        print(f"[v943.1] storage unavailable, every swap clip stays silent: {e}",
              flush=True)
        return 0
    if storage is None:
        print("[v943.1] storage not configured, every swap clip stays silent",
              flush=True)
        return 0

    done = 0
    for clip_id, key in wanted.items():
        row = by_id.get(clip_id)
        if row is None or not row.get("path"):
            print(f"[v943.1] clip id={clip_id} asked for source audio but is not "
                  f"in the export lineup — skipped", flush=True)
            continue
        render_path = Path(row["path"])
        if not render_path.exists():
            print(f"[v943.1] clip id={clip_id} render file missing at "
                  f"{render_path.name} — stays silent", flush=True)
            continue

        src_path = None
        out_path = None
        try:
            with _tf.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
                src_path = tmp.name
            # Streamed to disk by the storage backend, never held in memory —
            # this runs inside the same 2GB cgroup as the concat that OOMs.
            storage.download_file(key, src_path)
            size = os.path.getsize(src_path)
            if size > SWAP_SOURCE_MAX_BYTES:
                print(f"[v943.1] source {key} is {size} bytes, over the "
                      f"{SWAP_SOURCE_MAX_BYTES} cap — clip id={clip_id} stays silent",
                      flush=True)
                continue
            if not _v943_1_has_audio_stream(src_path):
                # Expected, not a fault: plenty of sources are silent.
                print(f"[v943.1] source {key} has NO audio stream — clip "
                      f"id={clip_id} stays silent (export continues)", flush=True)
                continue

            # EXPORT-SCOPED destination. Same name, different directory: the
            # canonical render under output_dir/ is only ever READ here.
            out_path = str(Path(export_dir) /
                           f"{render_path.stem}_v9431_{clip_id}.mp4")
            argv = _v943_1_mux_argv(
                render_path, src_path, out_path,
                render_duration=_v943_1_render_duration(render_path))
            proc = _sp.run(argv, capture_output=True, text=True, timeout=300)
            if proc.returncode != 0 or not os.path.exists(out_path):
                print(f"[v943.1] ffmpeg mux failed for clip id={clip_id} "
                      f"(rc={proc.returncode}): {(proc.stderr or '')[:300]} — "
                      f"clip stays silent, export continues", flush=True)
                continue
            # Only THIS export run's lineup follows the muxed copy.
            row["path"] = Path(out_path)
            # v943.1 + the timeline retime: tells video_processor this segment
            # now carries a restored track that must survive a speed change.
            row["swap_audio_restored"] = True
            out_path = None       # handed to the lineup; do not delete below
            done += 1
            # TEMP DIAG [TEMP] (remove once one v943.1 export is confirmed live)
            print(f"[TEMP][v943.1] muxed source audio for clip id={clip_id} "
                  f"into export copy {Path(row['path']).name} from key={key} "
                  f"(canonical {render_path.name} untouched)", flush=True)
        except Exception as e:
            print(f"[v943.1] source-audio mux skipped for clip id={clip_id}: {e} "
                  f"— clip stays silent, export continues", flush=True)
        finally:
            for p in (src_path, out_path):
                if p and os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass

    print(f"[v943.1] source-original audio: {done}/{len(wanted)} swap clip(s) "
          f"carried their source track into the export", flush=True)
    return done


async def _do_export_final(
    job_id: str,
    settings: ExportSettings,
    db: DBSession,
    current_user: User,
) -> dict:
    """v945.3 — own the export-scoped temp directory for the WHOLE call.

    The muxed-clip directory used to be created part-way down the export body
    and cleaned in a `finally` several hundred lines later. Everything raised
    in between — the VAD-not-installed 400, the no-valid-clip-files 400, any
    unexpected error — left the directory on disk, and Render's container
    keeps running after a failed export, so those leaks accumulate against the
    same disk the next export writes into.

    A directory created HERE, before anything can fail, and removed in this
    function's `finally`, has no exit path that skips the cleanup. Making it
    unconditionally is deliberate: an empty temp directory costs nothing, and
    a conditional one is how the gap opened in the first place.
    """
    import shutil as _sh9453
    import tempfile as _tf9453

    export_tmp_dir = _tf9453.mkdtemp(prefix=f"v9431_{str(job_id)[:8]}_")
    try:
        return await _do_export_final_impl(
            job_id, settings, db, current_user, export_tmp_dir)
    finally:
        # v943.1 — drop this export run's muxed clip copies. They exist only
        # so the source audio never had to be written over the canonical
        # render; nothing outside this call may still be reading them.
        try:
            _sh9453.rmtree(export_tmp_dir, ignore_errors=True)
        except Exception as _c9453:
            print(f"[v943.1] export copy cleanup skipped: {_c9453}", flush=True)


async def _do_export_final_impl(
    job_id: str,
    settings: ExportSettings,
    db: DBSession,
    current_user: User,
    export_tmp_dir: str,
) -> dict:
    """v850 — the export WORK. Was the body of POST /export-final until the
    durable queue landed. Unchanged logic; it just no longer runs inside the
    HTTP request (a Render deploy used to kill it mid-ffmpeg). Called by
    _export_runner() with its own long-lived DB session.

    Export all approved clips as a single final video.
    Optionally applies trimming and Voice Activity Detection (VAD).

    Works even after server restart by falling back to filesystem.

    Rules for start frame trimming:
    - Never trim start frames from the FIRST clip (clip_index 0)
    - Never trim start frames from clips that start a "cut" transition scene
    """
    from video_processor import export_final_video as process_export, check_vad_available
    # `or_` is not imported at module level in this file — every other user of it
    # imports it locally (see lines 4565, 5684, 13912, 15440, ...). _v892_not_plate
    # below closes over it, so without this line the export dies on a NameError the
    # moment it builds the clip query. Cost one production export run on 2026-08-13.
    from sqlalchemy import or_

    def _v892_not_plate():
        """Exclude composite_plate clips from the exported timeline.

        A composite frame is assembled from two renders: the performing layer
        and the background plate it is keyed over. Both are rendered so the
        operator has both pieces, but only the performing layer belongs in the
        concat — the plate is a layer, and concatenating it would insert a
        still frame as its own shot. Mirrors how a v698A audio twin renders
        without ever entering the visual timeline.
        """
        return or_(Clip.clip_role.is_(None), Clip.clip_role != 'composite_plate')

    # v865 — phase memory trace. The 2026-07-23 OOMs (2GB cgroup) gave no clue
    # which phase peaked because nothing measured container memory. These lines
    # bracket the heavy phases so the next incident names its own culprit.
    try:
        import mem_guard as _mg865
        _mg865.set_phase(f"export {job_id[:8]}")
        _mg865.log(f"export-start job={job_id[:8]}")
    except Exception:
        pass

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
                Clip.approval_status == "approved",
                # v892 — a composite_plate is a LAYER, not a shot. It renders so
                # the operator can key the performing layer over it, and it must
                # never be concatenated into the timeline as its own segment.
                _v892_not_plate(),
            ).order_by(Clip.clip_index).all()
    else:
        clips = db.query(Clip).filter(
            Clip.job_id == job_id,
            Clip.approval_status == "approved",
            _v892_not_plate(),   # v892 — layer, never a timeline segment
        ).order_by(Clip.clip_index).all()

    # TEMP DIAGNOSTIC (2026-08-13, remove once an export is confirmed green) — the
    # v892 clip query crashed on a missing `or_` import and produced no evidence at
    # all past "START". This line proves the query ran and says what it selected.
    print(f"[Export][v892] clip query OK: {len(clips)} clips selected for job={job_id}", flush=True)

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
                # v821 — carry the reworded Prompt B line + which variant
                # actually rendered, so export word-timing aligns against the
                # line that was SPOKEN (B when the clip rendered with Prompt B).
                "dialogue_text_b": clip.dialogue_text_b or None,
                "rendered_prompt_variant": clip.rendered_prompt_variant or "A",
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
    try:
        import video_processor as _vp872
        _vp872.mem_phase(f"export:download ({len(clips)} clips, 3 workers)")
    except Exception:
        pass
    with _TPE(max_workers=3) as pool:
        results = list(pool.map(_download_clip, list(enumerate(clips))))

    # v943.1 — lay the swap source's own audio back over any clip that asked
    # for it. Here and not later: the render files are on disk and each one is
    # still its own segment. ffmpeg + an R2 read are blocking, so this runs off
    # the event loop. It never raises — a clip that cannot get its audio stays
    # silent and the export goes on.
    # The muxed segments land in a directory that belongs to THIS export run
    # (see _v943_1_apply_source_audio) and are deleted by _do_export_final's
    # `finally`, after the concat and after every later pass that reads
    # clip_info. The canonical clip files under output_dir/ are never written.
    # v945.3 — the directory is created by the caller, so no exit path out of
    # this body (including the preflight 400s below) can leak it.
    _v9431_dir = export_tmp_dir
    try:
        await asyncio.to_thread(
            _v943_1_apply_source_audio, clips, [r for r in results if r],
            job.user_id, _v9431_dir)
    except Exception as _e9431:
        print(f"[v943.1] source-audio pass skipped entirely: {_e9431}", flush=True)

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

    # v888.1 — RESOLVE THE MUSIC BED BEFORE THE EXPORT STARTS.
    #
    # The first v888 export shipped an unscored video: upload-music wrote the
    # file to the request container's local disk, the export ran detached in a
    # DIFFERENT container, `Path(output_dir)/music_bed.mp3` did not exist there,
    # and video_processor logged "music not found" and carried on. Render's disk
    # is ephemeral; R2 is the only thing shared between the two. master_audio
    # already had exactly this recovery (see its block below) — the music path
    # did not.
    #
    # Two changes: recover from R2, and make a missing file a HARD 400. Silently
    # delivering a silent cut when the operator explicitly asked for a score is
    # the worst possible outcome — it looks like success.
    _music_path = None
    if settings.music_filename:
        _mp = output_dir / settings.music_filename
        if not _mp.exists() and storage:
            try:
                _r2 = f"jobs/{job_id}/outputs/{settings.music_filename}"
                if storage.exists(_r2):
                    print(f"[Export/v888.1] recovering music bed from R2: {_r2}", flush=True)
                    await asyncio.to_thread(storage.download_file, _r2, str(_mp))
            except Exception as _e:
                print(f"[Export/v888.1] music R2 recovery failed: {_e}", flush=True)
        if not _mp.exists():
            raise HTTPException(
                status_code=400,
                detail=(f"Music bed '{settings.music_filename}' not found on disk or in R2 "
                        f"for this job. Re-upload it via POST /api/jobs/{job_id}/upload-music."))
        _music_path = str(_mp)
        print(f"[Export/v888.1] music bed resolved: {_mp} "
              f"({_mp.stat().st_size / 1e6:.1f}MB) start={settings.music_start_s}s "
              f"mode={settings.music_mode} gain={settings.music_gain_db:+.1f}dB", flush=True)

    # v890 — BEAT ALIGNMENT. Runs HERE, before any clip work, so its ~120MB
    # analysis peak never stacks with ffmpeg or the Whisper model.
    #
    # Default OFF: with no song, or with a song but beat_align off, the authored
    # target_duration_s from the md drives the cut exactly as written (v889).
    # ON: each authored cut NUDGES to the nearest strong beat, bounded by
    # beat_tol_beats, so shot lengths and caption timing survive.
    #
    # The analysis is WINDOWED to the span this video needs (measured: 121MB /
    # 1.9s for 45s, vs 678MB / 31s for a full 200s track). HPSS is kept — without
    # it the tempo comes out 3:2 wrong.
    _music_start_eff = float(settings.music_start_s)
    _beat_report = {"beat_align": False}
    if settings.beat_align and _music_path:
        # BaseException, not Exception. The first version caught Exception and
        # analyze_song raised SystemExit (BaseException) when librosa was
        # missing — it escaped, killed the export, and the queue retried it into
        # a crash loop that SIGABRT'd gunicorn three times (2026-08-04).
        # Alignment is an enhancement; it must never be able to lose an export.
        try:
            import json as _json
            import beat_align as _ba
            _ordered = sorted(clip_info, key=lambda c: c.get("_order", 0))
            _targets = [c.get("target_duration_s") for c in _ordered]
            if not all(isinstance(t, (int, float)) and t and t > 0 for t in _targets):
                _missing = [i + 1 for i, t in enumerate(_targets)
                            if not (isinstance(t, (int, float)) and t and t > 0)]
                raise ValueError(
                    f"beat_align needs an authored target_duration_s on every clip; "
                    f"clip(s) {_missing} have none")
            _total = float(sum(_targets))
            # v890.2 — analyse ON-BOX, but only ever a WINDOW, and cache it.
            # This has to work for a user who just drags in a song, so a CLI
            # step or a local worker is not an option.
            #
            # THREE SAFEGUARDS, because the box is 2GB with an OOM history:
            #  (1) WINDOW — [music_start-2s, +authored_total+8s] only. Measured
            #      121MB/1.9s for 45s vs 755MB/18.9s for a full 200s track.
            #      Downsampling instead is not viable (11kHz gives 99.38 BPM
            #      against a true 95.70) and neither is extrapolating from one
            #      window (0.30s error late in a track, ~half a beat).
            #  (2) CACHE — keyed by song content hash + the exact window, so a
            #      re-export or a reused track never re-analyses.
            #  (3) GATE — skip if free memory is low. Falling back to the
            #      authored timings is always correct; an OOM is not.
            _win_off = max(0.0, float(settings.music_start_s) - 2.0)
            _win_dur = _total + 8.0
            _sig = hashlib.sha256()
            with open(_music_path, "rb") as _f:
                for _chunk in iter(lambda: _f.read(1 << 20), b""):
                    _sig.update(_chunk)
            _sig.update(f"|{_win_off:.3f}|{_win_dur:.3f}|{settings.beat_beats_per_bar}|v890.6".encode())
            _grid_path = output_dir / f"beatgrid_{_sig.hexdigest()[:16]}.json"

            if _grid_path.exists():
                _an = _json.loads(_grid_path.read_text(encoding="utf-8"))
                print(f"[Export/v890] beat grid CACHE HIT {_grid_path.name} "
                      f"({_an.get('bpm')} BPM) - no analysis", flush=True)
            else:
                # mem_guard.headroom_ok trims the heap first, then compares the
                # cgroup figure; it returns True on local dev where the host
                # number is not a container headroom number.
                _ok, _snap = True, {}
                try:
                    import mem_guard as _mg
                    _ok, _snap = _mg.headroom_ok(500)
                except Exception as _mge:
                    print(f"[Export/v890] mem gate unavailable ({_mge}); proceeding",
                          flush=True)
                if not _ok:
                    raise MemoryError(
                        f"insufficient headroom for beat analysis "
                        f"(avail={_snap.get('avail_mb')}MB, need 500MB free for a "
                        f"~150MB window). The authored timings still apply.")
                print(f"[Export/v890] analysing {Path(_music_path).name} "
                      f"window [{_win_off:.1f}s +{_win_dur:.1f}s] "
                      f"(avail={_snap.get('avail_mb')}MB)", flush=True)
                _an = await asyncio.to_thread(
                    _ba.analyze_song, Path(_music_path),
                    beats_per_bar=settings.beat_beats_per_bar,
                    offset=_win_off, duration=_win_dur)
                try:
                    _grid_path.write_text(_json.dumps(_an), encoding="utf-8")
                except Exception as _ce:
                    print(f"[Export/v890] grid cache write failed (non-fatal): {_ce}",
                          flush=True)
            _scenes = [(i + 1, t) for i, t in enumerate(_targets)]
            _mode = (settings.beat_mode or "snap").lower()
            _bt = _an["beat_times"]
            _sal = _an["beat_salience"]
            _music_start = float(settings.music_start_s)
            _drop_used = None

            if _mode == "solve":
                # v890.6 — the full v5 solve. The music picks every length inside
                # [min,max]; clips before the drop are laid out BACKWARDS from it
                # so beat_drop_clip lands on the drop exactly, and pacing curves
                # accelerate into it. Authored lengths are discarded by design.
                import numpy as _np
                _n = len(_scenes)
                _dc = settings.beat_drop_clip or max(1, _n // 2)
                _dc = max(1, min(_n, int(_dc)))
                if settings.beat_drop_time is not None:
                    _drop_used = float(settings.beat_drop_time)
                else:
                    _drops = _an.get("drops") or []
                    if not _drops:
                        raise ValueError("no drop detected in this window; set an exact "
                                         "drop time or use snap mode")
                    _drop_used = float(_drops[min(settings.beat_drop_rank,
                                                  len(_drops)) - 1]["time_seconds"])
                _anchor = int(_np.abs(_np.array(_bt) - _drop_used).argmin())
                # v5 split_pins_by_drop: clip K < drop -> before-block position
                # K-1; clip K >= drop -> after-block position K - drop.
                _pins_raw = {int(k): float(v) for k, v in (settings.beat_pins or {}).items()}
                _pins_pre = {k - 1: v for k, v in _pins_raw.items() if k < _dc}
                _pins_post = {k - _dc: v for k, v in _pins_raw.items() if k >= _dc}
                _pre = _ba.solve_boundaries(_bt, _sal, _anchor, _dc - 1,
                                            settings.beat_min_s, settings.beat_max_s,
                                            before=True, pins=_pins_pre)
                _post = _ba.solve_boundaries(_bt, _sal, _anchor, _n - _dc + 1,
                                             settings.beat_min_s, settings.beat_max_s,
                                             before=False, pins=_pins_post)
                _edges = ([float(_bt[i]) for i in _pre[:-1]] if _pre else []) + \
                         [float(_bt[i]) for i in _post]
                if len(_edges) != _n + 1:
                    raise ValueError("solver built %d edges for %d clips"
                                     % (len(_edges), _n))
                _music_start = _edges[0]   # the song is trimmed to the edit window
                print(f"[Export/v890] SOLVE: drop {_drop_used:.2f}s -> beat {_anchor} "
                      f"@ {_bt[_anchor]:.2f}s, clip {_dc} lands on it, "
                      f"range {settings.beat_min_s}-{settings.beat_max_s}s", flush=True)
            else:
                _edges = _ba.snap_boundaries(
                    _scenes, _bt, _sal,
                    start_time=_music_start,
                    tol_beats=settings.beat_tol_beats,
                )

            _new = [round(_edges[i + 1] - _edges[i], 3) for i in range(len(_scenes))]
            # v890.6 — per-clip speed. v888's retime fills a slot exactly:
            # source_used = min(src, target*speed), actual = source_used/target.
            # Before the drop clip we use pre_drop_speed, from it onward
            # post_drop_speed, both times the global clip_speed.
            _dropclip = (settings.beat_drop_clip or 0) if _mode == "solve" else 0
            for _i, (_c, _nd) in enumerate(zip(_ordered, _new), 1):
                _c["target_duration_s"] = _nd
                _base = (settings.beat_post_drop_speed if (_dropclip and _i >= _dropclip)
                         else settings.beat_pre_drop_speed)
                _sp = round(_base * settings.beat_clip_speed, 4)
                if abs(_sp - 1.0) > 1e-6:
                    _c["clip_speed"] = _sp
                if not (_c.get("cut_mode") or "").lower() == "timeline":
                    _c["cut_mode"] = "timeline"   # aligned targets must be applied
            # solve trims the song to the edit window, so the bed must start there
            if _mode == "solve":
                _music_start_eff = _music_start
            _beat_report = {
                "beat_align": True, "mode": _mode,
                "drop_used_s": round(_drop_used, 3) if _drop_used is not None else None,
                "drops_detected": [round(float(d["time_seconds"]), 2)
                                   for d in (_an.get("drops") or [])[:5]],
                "music_start_s": round(_music_start_eff, 3),
                "clip_speed_applied": round(settings.beat_clip_speed, 3),
                "bpm": round(_an["bpm"], 2),
                "bar_seconds": round(4 * 60.0 / _an["bpm"], 3),
                "tol_beats": settings.beat_tol_beats,
                "authored_total_s": round(_total, 3),
                "aligned_total_s": round(sum(_new), 3),
                "per_clip": [{"clip": i + 1, "authored": _targets[i], "aligned": _new[i],
                              "delta": round(_new[i] - _targets[i], 3)}
                             for i in range(len(_new))],
            }
            print(f"[Export/v890] beat align: {_an['bpm']:.2f} BPM, "
                  f"{_total:.2f}s -> {sum(_new):.2f}s, max nudge "
                  f"{max(abs(n - t) for n, t in zip(_new, _targets)):.3f}s", flush=True)
            for _i, (_t, _n) in enumerate(zip(_targets, _new), 1):
                print(f"[Export/v890]   clip {_i}: {_t:.3f}s -> {_n:.3f}s "
                      f"({_n - _t:+.3f})", flush=True)
        except BaseException as _be:
            # Beat alignment is an ENHANCEMENT. Never lose an export over it —
            # fall back to the authored timings, which are always valid.
            # BaseException on purpose: see the note above the try.
            print(f"[Export/v890] beat align FAILED ({type(_be).__name__}: {_be}) - "
                  f"falling back to the authored md timings", flush=True)
            _beat_report = {"beat_align": False,
                            "beat_align_error": f"{type(_be).__name__}: {_be}"[:300]}
    
    # Create output filename with unique suffix to prevent collisions
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_suffix = uuid.uuid4().hex[:6]  # 6 char random suffix
    # v856 — STAMP THE JOB ID INTO THE NAME WE MINT.
    #
    # The operator downloads this mp4 and drops it in a watched folder, and the
    # watcher then has to work out which job it came from — by transcribing it
    # and comparing words. Builds share their script bank VERBATIM, so that
    # comparison regularly cannot separate two jobs, and a day of wrong links
    # is what a coin-flip between twins buys you.
    #
    # We named this file. So we write the answer on it: the watcher reads the
    # id straight back out (instagram_match.job_id_from_filename) and the match
    # becomes a lookup that cannot be wrong.
    #
    # The id goes AFTER the prefix, never before it: `final_export_` /
    # `final_broll_` / `export_` is how ~6 other places detect "this is a final
    # export" (main.py has_export self-heal, export_probe._FINAL_PREFIXES, the
    # UI's export list). Inserting inside the name leaves every one of them
    # working. A non-uuid job id yields no segment and we mint the legacy shape
    # — the watcher just falls back to evidence, as it does for any older file.
    _job_seg = _ig_match.export_job_segment(str(job_id))
    _job_part = f"{_job_seg}_" if _job_seg else ""
    output_filename = f"final_export_{_job_part}{timestamp}_{unique_suffix}.mp4"
    output_path = output_dir / output_filename
    # v858 — write the export basename onto the Job row so a folder file whose
    # NAME embeds this basename resolves to its job by a plain equality lookup
    # (instagram_match.export_basename_from_filename -> Job.export_basename),
    # with no waveform and no transcription. Persisted with the job's own commit
    # further down (job.has_export = True; db.commit()). The R2 object is stored
    # under this same name (jobs/<id>/outputs/<output_filename>), so basename ==
    # Path(key).stem, which is exactly what the lazy backfill in export_probe
    # stamps on historical jobs.
    job.export_basename = output_filename[:-4]  # strip ".mp4"
    print(f"[Export][v856] minted {output_filename} (job={str(job_id)[:8]} "
          f"stamped={'yes' if _job_seg else 'no'} basename={job.export_basename})", flush=True)  # TEMP DIAG
    
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

        # v925 — declared at export scope: the v698A b-roll pipeline now runs
        # at the END of this function (after enhancement + speed), so the flag
        # has to survive past the branch that sets it.
        _has_v698a_pairs = False

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
                # v821 — fingerprint against the line that was actually SPOKEN
                # (Prompt B's reworded line when the clip rendered with B).
                # clips are Clip ORM rows, so wrap in a dict for the shared
                # active_dialogue_line() helper (logic lives in ONE place).
                _spoken = active_dialogue_line({
                    "dialogue_text": clip.dialogue_text,
                    "dialogue_text_b": clip.dialogue_text_b,
                    "rendered_prompt_variant": clip.rendered_prompt_variant,
                })
                dialogue_lines.append(_apply_prefix(_spoken or ""))
            
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
                    music_path=_music_path,
                    music_start_s=_music_start_eff,
                    music_gain_db=settings.music_gain_db,
                    music_mode=settings.music_mode,
                    transition=settings.transition,
                    transition_duration=settings.transition_duration,
                    dialogue_texts=[
                        active_dialogue_line(c) or "" for c in _speaker_clip_info
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

                # === v698A Phase 4b-ii — BROLL PIPELINE ===
                # v925 (2026-08-08): MOVED to the end of this function, after
                # audio enhancement + the speed pass. See the block tagged
                # "v925 — B-ROLL PIPELINE" below for why.
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
                    music_path=_music_path,
                    music_start_s=_music_start_eff,
                    music_gain_db=settings.music_gain_db,
                    music_mode=settings.music_mode,
                    transition=settings.transition,
                    transition_duration=settings.transition_duration,
                    dialogue_texts=[active_dialogue_line(c) or "" for c in clip_info],
                    language=json.loads(job.config_json).get("language", "English") if job.config_json else "English",
                    cut_prefix_audio=False,
                    prefix_word=_prefix_word,
                )

        print(f"[Export] Success! Stats: {stats}")

        # v872 — phase boundary. The concat pass is done and its Whisper models,
        # decoded-audio buffers and ffmpeg temp churn are dead objects now; the
        # audio-enhancement and support-track phases below are the next big
        # allocators. Give the pages back BEFORE they run rather than stacking
        # one phase's garbage under the next phase's peak.
        _v864_release("post-concat")

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
                try:
                    import video_processor as _vp872b
                    _vp872b.mem_phase(f"export:audio ({', '.join(enabled_steps)})")
                except Exception:
                    pass


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
        # v925 — cap raised 90s → 300s. The 90s cap was a v692b OOM guard
        # against buffering MBs of ffmpeg stderr under capture_output=True when
        # a broken concat produced a 233s file. The real fix is to stop ffmpeg
        # printing: `-loglevel error -nostats` below makes the captured stderr a
        # few bytes, so length no longer decides. The old cap silently dropped
        # the speed pass on every export over 90s — the operator's 97s and 101s
        # exports shipped at 1.0x (and, pre-v925, with a 1.1x b-roll beside
        # them). 300s stays as a sanity backstop for a genuinely broken concat.
        _SPEED_MAX_DURATION_S = 300.0
        _final_dur_safe = float(stats.get("final_duration") or 0.0)
        _speed_safe = (
            settings.playback_speed and settings.playback_speed > 1.01
            and not settings.master_audio_filename
            and _final_dur_safe > 0
            and _final_dur_safe <= _SPEED_MAX_DURATION_S
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
                    # v925 — silence ffmpeg's progress spam. capture_output=True
                    # buffers stderr in this process; with the default loglevel
                    # a long file writes MBs of `frame=` lines into RAM, which
                    # is what the old 90s length cap was really guarding.
                    "ffmpeg", "-y", "-loglevel", "error", "-nostats",
                    "-i", str(output_path),
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
                    # v813 — final_duration used to stay at the PRE-speed
                    # value after the speed pass, so the export card (and
                    # the saved card metadata) showed the unsped duration.
                    # A 1.1× export looked like speed never applied. Keep
                    # the pre-speed value under its own key and re-probe
                    # the sped file for the real final_duration.
                    stats["pre_speed_duration"] = _final_dur_safe
                    try:
                        from video_processor import ffprobe_json as _fpj, get_duration as _gd
                        stats["final_duration"] = _gd(_fpj(output_path))
                    except Exception:
                        stats["final_duration"] = round(_final_dur_safe / speed, 3)
                    print(
                        f"[Export] Speed applied: {speed}× "
                        f"({_final_dur_safe:.2f}s → {stats['final_duration']:.2f}s)",
                        flush=True,
                    )
                else:
                    print(f"[Export] Speed change failed: {result.stderr.decode()[:200]}", flush=True)
            except Exception as e:
                print(f"[Export] Speed change error (non-fatal): {e}", flush=True)

        # === v925 — B-ROLL PIPELINE (v698A Phase 4b-ii, relocated) ===
        #
        # WHY IT LIVES HERE NOW. Until v925 this ran immediately after the
        # speaker concat — BEFORE audio enhancement and BEFORE the speed pass —
        # so it had to PREDICT what the speaker file would become. It predicted
        # the speed by re-deriving the same condition the speed pass uses
        # (v773.10.18: pre-apply `atempo=playback_speed` to the extracted master
        # audio and scale every target by 1/speed), and it predicted the master
        # timeline by SUMMING the per-clip pre-normalize durations.
        #
        # Both predictions were wrong in production:
        #   1. The speaker's speed gate carries TWO extra conditions the b-roll
        #      copy did not: the v692b `final_duration <= 90s` OOM guard, and
        #      "the ffmpeg speed command actually succeeded" (its failure is
        #      swallowed as non-fatal). Any export over the cap → speaker stays
        #      1.0x while the b-roll already went 1.1x.
        #      Measured on the operator's own downloads (2026-08-08):
        #        d8051bf6 — broll 88.511s vs speaker 97.291s (ratio 1.0992)
        #        0bd0acf8 — broll 91.962s vs speaker 101.161s (ratio 1.1000)
        #      Audio-envelope correlation between the two files of a pair peaks
        #      at time-scale 1.100 (corr 0.976) and is 0.004 at scale 1.000 —
        #      i.e. the same audio 1.1x faster, out of sync from second 0.
        #      5 of 8 downloaded pairs carry that signature; the 2 clean pairs
        #      were exports under the 90s cap.
        #   2. Sum-of-per-clip-durations != the real concat timeline. The clips
        #      are probed BEFORE concat_videos re-encodes each one to fps=24 +
        #      48k AAC; the normalized files are slightly longer. Measured with
        #      ffmpeg on an 8-clip / 40s case: +67ms by the last clip, monotonic,
        #      and it scales with clip count.
        #
        # v925 replaces prediction with measurement:
        #   - the master audio is extracted from the FINISHED speaker file, so
        #     whatever enhancement/speed did to it is already baked in;
        #   - targets are scaled by the MEASURED ratio (final speaker duration /
        #     sum of the per-clip durations the targets were built from), which
        #     absorbs the speed pass AND the concat drift in one number;
        #   - both outputs are ffprobed at the end and a mismatch is reported
        #     loudly instead of shipping a silently desynced pair.
        #
        # Each broll visual is placed at the timestamp where ITS dialogue line
        # plays in the master audio:
        #   - 'visual_pair' → dialogue = clip.voiceover_line
        #   - 'single' (HOOK/CTA) → not in broll; that window renders BLACK
        #   - 'audio_pair'  → SKIP (face-anchor visual, not in broll)
        #   - 'text_card'   → SKIP (no dialogue to align; gap → black)
        # Visuals get speed-adjusted (up to 2x cap) to fit each line's span.
        if _has_v698a_pairs:
            try:
                # explicit malloc_trim before the b-roll's ffmpeg work — the
                # speaker's Whisper-tiny (v701y) was disposed at the end of its
                # per-clip loop but glibc may still hold the freed pages.
                try:
                    import ctypes as _ct
                    _ct.CDLL("libc.so.6").malloc_trim(0)
                    print(
                        "[Export/v698A/broll] malloc_trim applied "
                        "before master-audio pipeline",
                        flush=True,
                    )
                except Exception:
                    pass

                from video_processor import export_with_master_audio
                from video_processor import (
                    ffprobe_json as _fpj_b,
                    get_duration as _gd_b,
                )
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

                # v701zf — broll includes ONLY visual_pair clips. HOOK + CTA
                # (singles) are persona on-camera → those windows on the master
                # timeline render as black in broll (audio plays, no replacement
                # visual). audio_pair + text_card stay skipped. visual_pair
                # without a resolvable paired_clip_id (no audio_pair sibling in
                # speaker) is also skipped.
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

                if not broll_clip_info:
                    raise RuntimeError(
                        "no visual_pair clips resolved for the b-roll output"
                    )

                # v925 — MEASURE the finished speaker file. No atempo
                # simulation: the speed pass (if any) already ran above, so the
                # audio we extract here IS the audio of the delivered speaker
                # video, sample for sample.
                broll_temp_dir = Path(_tmp.mkdtemp(prefix="v698a_broll_"))
                speaker_master_audio = broll_temp_dir / "speaker_master.mp3"
                import subprocess as _sp
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
                try:
                    _speaker_final_dur = float(_gd_b(_fpj_b(output_path)))
                except Exception as _sd_err:
                    print(
                        f"[Export/v698A/broll] speaker probe failed: {_sd_err}",
                        flush=True,
                    )
                    _speaker_final_dur = 0.0
                print(
                    f"[Export/v698A/broll] extracted speaker master audio: "
                    f"{speaker_master_audio.name} "
                    f"({speaker_master_audio.stat().st_size // 1024}KB) from a "
                    f"FINISHED speaker of {_speaker_final_dur:.3f}s "
                    f"(speed_applied={stats.get('playback_speed') or 'none'}, "
                    f"audio_enhanced={stats.get('audio_enhanced')})",
                    flush=True,
                )

                broll_filename = output_filename.replace(
                    "final_export_", "final_broll_"
                )
                if broll_filename == output_filename:
                    broll_filename = f"final_broll_{output_filename}"
                broll_output_path = output_dir / broll_filename

                # v701zd — build targets from the speaker's per-clip post-VAD
                # durations. The speaker pipeline already trimmed each clip via
                # Whisper-VAD; those durations ARE the pre-normalize master
                # timeline. No second Whisper master transcription needed (that
                # legacy path repeatedly under-transcribed to ~half the script
                # words and bricked alignment).
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
                    #   - visual_pair: match by paired_clip_id (the audio_pair
                    #     sibling held that position in the speaker concat)
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
                                f"[Export/v698A/broll] ⚠ no speaker position "
                                f"for broll clip clip_index={_bc.get('clip_index')} "
                                f"role={_role} lookup_id={_lookup_id} — falling back "
                                f"to the Whisper-master path (known-weak alignment)",
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
                        # === v926 — map the windows through what actually
                        # happened to the timeline, in two exact steps. ===
                        #
                        # The targets above live on the PRE-VAD concat clock
                        # (cumulative sums of the per-clip durations). Two
                        # things move them before delivery:
                        #
                        #   1. the final-pass silence VAD, which deletes
                        #      silence from WHEREVER it sits — non-uniform, so
                        #      no single ratio can express it. Measured on job
                        #      d8051bf6: 24.175s cut out of 121.47s. v925
                        #      rescaled by one global ratio, which fixed total
                        #      length (delta 0.001s) but still left b-roll
                        #      visual 3 sitting 2.22s past the end of its own
                        #      line, on top of the persona's next line.
                        #      apply_vad now returns `keep_segments` — the
                        #      exact ranges it kept, in pre-VAD time — so a
                        #      timestamp maps across by summing kept time
                        #      before it. Exact, not approximated.
                        #   2. the speed pass, a genuine uniform scale, taken
                        #      as the MEASURED ratio of the delivered duration
                        #      to the post-VAD one.
                        #
                        # No VAD keep-list (per-clip Whisper path, where the
                        # concat is never globally re-trimmed) → step 1 is the
                        # identity and step 2 falls back to the delivered
                        # duration over the summed clip durations, which also
                        # absorbs concat normalize drift.
                        _sum_durs = sum(float(_d or 0.0) for _d in _speaker_durs)
                        _keep = stats.get("keep_segments") or []
                        _post_vad_dur = float(
                            stats.get("pre_speed_duration")
                            or stats.get("vad_final_duration")
                            or 0.0
                        ) or _speaker_final_dur

                        from video_processor import (
                            map_time_through_keep_segments as _map_through_vad_impl,
                        )

                        def _map_through_vad(_t_pre, _segs=_keep):
                            return _map_through_vad_impl(_t_pre, _segs)

                        if _keep:
                            _k = (
                                _speaker_final_dur / _post_vad_dur
                                if _post_vad_dur > 0.1 else 1.0
                            )
                            _mode = f"vad_keep_map({len(_keep)} segs) × {_k:.4f}"
                            for _t in _pre_targets:
                                _t["start"] = _map_through_vad(_t["start"]) * _k
                                _t["end"] = _map_through_vad(_t["end"]) * _k
                                _t["target_duration"] = _t["end"] - _t["start"]
                        else:
                            _k = 1.0
                            if _sum_durs > 0.1 and _speaker_final_dur > 0.1:
                                _k = _speaker_final_dur / _sum_durs
                            if _k <= 0.4 or _k >= 1.6:
                                print(
                                    f"[Export/v698A/broll] ⚠ measured ratio {_k:.4f} "
                                    f"outside sane range (sum_clips={_sum_durs:.3f}s, "
                                    f"speaker={_speaker_final_dur:.3f}s) — using 1.0. "
                                    f"Something upstream changed the timeline length.",
                                    flush=True,
                                )
                                _k = 1.0
                            _mode = f"global ratio {_k:.4f}"
                            if abs(_k - 1.0) > 1e-4:
                                for _t in _pre_targets:
                                    _t["start"] = _t["start"] * _k
                                    _t["end"] = _t["end"] * _k
                                    _t["target_duration"] = _t["end"] - _t["start"]
                        stats["v698a_broll_time_scale"] = round(_k, 6)
                        stats["v698a_broll_target_map"] = _mode
                        print(
                            f"[Export/v698A/broll] targets built from speaker "
                            f"per-clip durations ({len(_pre_targets)} clips) and "
                            f"mapped by {_mode} "
                            f"(pre-VAD sum={_sum_durs:.3f}s → post-VAD "
                            f"{_post_vad_dur:.3f}s → delivered "
                            f"{_speaker_final_dur:.3f}s)",
                            flush=True,
                        )
                        for _i, _t in enumerate(_pre_targets):
                            print(
                                f"[Export/v698A/broll]   window {_i}: "
                                f"{_t['start']:.2f}s → {_t['end']:.2f}s",
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
                stats["v698a_broll_mode"] = "master_audio_alignment_v925"
                stats["v698a_broll_stats"] = broll_stats

                # v925 — CLOSE THE LOOP. A predicted pair is how this desynced
                # for months without anyone being told. Measure both delivered
                # files and say it out loud when they disagree.
                try:
                    _broll_dur = float(_gd_b(_fpj_b(broll_output_path)))
                except Exception:
                    _broll_dur = 0.0
                _len_delta = _broll_dur - _speaker_final_dur
                stats["v698a_broll_duration"] = round(_broll_dur, 3)
                stats["v698a_speaker_duration"] = round(_speaker_final_dur, 3)
                stats["v698a_broll_length_delta"] = round(_len_delta, 3)
                if abs(_len_delta) > 0.15:
                    stats["v698a_broll_length_mismatch"] = True
                    _ratio = (
                        _speaker_final_dur / _broll_dur if _broll_dur > 0.01 else 0.0
                    )
                    print(
                        f"[Export/v698A/broll] ❌ LENGTH MISMATCH: broll "
                        f"{_broll_dur:.3f}s vs speaker {_speaker_final_dur:.3f}s "
                        f"(delta {_len_delta:+.3f}s, speaker/broll ratio "
                        f"{_ratio:.4f}). The two files will NOT line up on the "
                        f"editor timeline — treat this b-roll as unusable and "
                        f"read the [Export] Speed check line above.",
                        flush=True,
                    )
                else:
                    stats["v698a_broll_length_mismatch"] = False
                    print(
                        f"[Export/v698A/broll] length check OK: broll "
                        f"{_broll_dur:.3f}s vs speaker {_speaker_final_dur:.3f}s "
                        f"(delta {_len_delta:+.3f}s)",
                        flush=True,
                    )

                # v925 — PERSIST the verdict. Every [Export] line above is a
                # print(): it reaches Render's stdout and nowhere else, so the
                # only way anyone could check a shipped pair was to download
                # both files and probe them by hand (job_logs held 0 rows for
                # the export phase). Write the one line that matters into
                # job_logs so `GET /api/jobs/{id}/logs` can answer "did this
                # pair line up?" without shell access to the box. Note the
                # 24h purge for completed jobs — this is a verification aid
                # for the export you just ran, not an archive.
                try:
                    _verdict = (
                        "❌ b-roll LENGTH MISMATCH"
                        if stats.get("v698a_broll_length_mismatch")
                        else "✓ b-roll length matches speaker"
                    )
                    add_job_log(
                        db, job_id,
                        f"{_verdict}: broll {_broll_dur:.3f}s vs speaker "
                        f"{_speaker_final_dur:.3f}s (delta {_len_delta:+.3f}s, "
                        f"speed={stats.get('playback_speed') or 'none'}, "
                        f"time_scale={stats.get('v698a_broll_time_scale')}, "
                        f"clips={len(broll_clip_info)})",
                        "WARNING" if stats.get("v698a_broll_length_mismatch") else "INFO",
                        "export",
                        details={
                            "broll_duration": stats.get("v698a_broll_duration"),
                            "speaker_duration": stats.get("v698a_speaker_duration"),
                            "length_delta": stats.get("v698a_broll_length_delta"),
                            "time_scale": stats.get("v698a_broll_time_scale"),
                            "playback_speed": stats.get("playback_speed"),
                            "broll_filename": broll_filename,
                            "targets": "pre_computed" if _pre_targets else "whisper_master",
                        },
                    )
                except Exception as _jl_err:
                    print(
                        f"[Export/v698A/broll] job-log write failed (non-fatal): "
                        f"{_jl_err}",
                        flush=True,
                    )

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

        # === v948 — POST-CONCAT SILENCE-HOLE SWEEP ===
        #
        # The per-clip whisper VAD trims each clip's own edges. It cannot see
        # a pause in the middle of a clip, and it cannot see the stack-up at a
        # clip boundary (clip N's kept tail + clip N+1's kept head), so both
        # survive into the assembled final as dead air. Operator rule
        # (wiki/meta/generate-video-checklist.md "No dead air"): a selling
        # final ships with ZERO silences >= 0.9s and every hole is cut to a
        # ~0.3s breath — jump cuts are native to the format. Done by hand on
        # job 29d45418 (50.3s -> 44.0s, zero detections after); this is that
        # procedure as a stage.
        #
        # WHERE IT SITS. Last thing before the file is stored/uploaded, so it
        # runs on the timeline that actually ships: after concat, after audio
        # enhancement, after the speed pass, after the v925 b-roll pass.
        # Anything earlier would sweep a file that later stages then change.
        #
        # OFF unless settings.max_silence_s is set — absent means the export
        # is byte-identical to pre-v948.
        #
        # NEVER fatal: on any error the pre-sweep file ships untouched. An
        # export that survived the whole pipeline is not worth losing to a
        # cosmetic pass.
        _v948_max = getattr(settings, "max_silence_s", None)
        if _v948_max and _v948_max > 0:
            try:
                import os as _os948
                from video_processor import sweep_silence_holes as _sweep948
                _swept_path = output_dir / f"swept_{output_filename}"
                _sweep = await asyncio.to_thread(
                    _sweep948, output_path, _swept_path, float(_v948_max),
                )
                if _sweep.get("applied"):
                    _os948.replace(_swept_path, output_path)
                    stats["v948_holes_cut"] = _sweep["holes_cut"]
                    stats["v948_removed_s"] = round(_sweep["removed_s"], 3)
                    stats["v948_residual"] = _sweep["residual"]
                    stats["pre_sweep_duration"] = _sweep["original_duration"]
                    stats["final_duration"] = _sweep["final_duration"]
                print(
                    f"[Export/v948] hole sweep: {_sweep['holes_cut']} holes cut, "
                    f"{_sweep['removed_s']:.1f}s removed, "
                    f"residual detections={_sweep['residual']}",
                    flush=True,
                )
                if _sweep.get("residual"):
                    # Say it out loud rather than reporting a clean sweep that
                    # the file does not support. Usually means a hole sits
                    # inside a segment the plan kept whole (detector floor /
                    # threshold mismatch), not that the cut failed.
                    print(
                        f"[Export/v948] ⚠ {_sweep['residual']} silence(s) >= "
                        f"{_v948_max}s STILL in the shipped file: "
                        f"{_sweep.get('residual_holes')}",
                        flush=True,
                    )
                if _sweep.get("applied") and stats.get("v698a_broll_duration"):
                    # The v925 b-roll track was measured against the speaker
                    # file as it stood BEFORE this cut, so it is now longer
                    # than what ships. Surface it — do not pretend the pair
                    # still lines up.
                    print(
                        f"[Export/v948] ⚠ b-roll was built against the "
                        f"pre-sweep speaker file; it is now "
                        f"{_sweep['removed_s']:.1f}s longer than the shipped "
                        f"master. Re-run the b-roll pass if you need the pair.",
                        flush=True,
                    )
                    stats["v948_broll_stale"] = True
            except Exception as _sweep_err:
                print(
                    f"[Export/v948] hole sweep FAILED ({_sweep_err}) — "
                    f"shipping the unswept final",
                    flush=True,
                )
                stats["v948_error"] = str(_sweep_err)[:500]

        # Upload to R2 for persistence (voice swap needs this as input after Render restarts)
        try:
            import video_processor as _vp872c
            _vp872c.mem_phase("export:upload")
        except Exception:
            pass
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

        # v825 — timed support-image inserts: emit a second SILENT track
        # (stills at their word-spans) beside the talking-head master, for
        # post-production compositing. Fully guarded — never breaks export.
        #
        # v864 — MEMORY SAFETY. This step loads faster-whisper "small"
        # (~250MB resident) to word-align the master audio. Its own comment in
        # video_processor.transcribe_master_audio assumes "the speaker's tiny is
        # disposed + malloc_trim before this load runs" — the export path never
        # honoured that contract. On 2026-07-23 a 12-clip export ran this while
        # an image job was uploading variants on the same instance: the box was
        # OOM-killed mid-align (log: MasterAlign transcribed 240 words, two
        # [Support] placements, then "Starting gunicorn" + instance restarted).
        # The try/except below never fired because the process died outright,
        # so the whole export result was lost with it.
        #
        # Three guards, cheapest first:
        #   1. release memory to the OS BEFORE the model load (gc + malloc_trim)
        #   2. refuse to load when MemAvailable is too low — skip the track and
        #      KEEP the export, instead of killing the instance
        #   3. serialize the step so two exports never hold a model at once
        support_track_info = {}
        try:
            from image_platform import ImageJobBatch, ImageNode, ImageVariant, parse_scene_table as _pst, images_root as _iroot, _storage_download_to_local as _dl2
            import re as _re2, subprocess as _sp2
            _sb = db.query(ImageJobBatch).filter(
                ImageJobBatch.promoted_video_job_id == job_id
            ).first()
            _smd = _sb.source_markdown if _sb else None
            _parsed = _pst(_smd) if _smd else {}  # parse the build ONCE; reused below
            _sup = (_parsed.get("support_inserts") if _smd else []) or []
            print(f"[Export][v825] support_inserts={len(_sup)}", flush=True)  # TEMP DIAG
            if _sup and _sb:
                from video_processor import (
                    transcribe_master_audio as _tma,
                    resolve_support_spans as _rss,
                    export_support_track as _est,
                    ffprobe_json as _fpj, get_duration as _gd,
                )
                # 1) master audio from the final talking-head mp4
                _sup_audio = output_dir / "support_master.mp3"
                _ac = ["ffmpeg", "-y", "-i", str(output_path), "-vn",
                       "-acodec", "libmp3lame", "-q:a", "2", str(_sup_audio)]
                _acr = await asyncio.to_thread(_sp2.run, _ac, capture_output=True, text=True)
                if _acr.returncode != 0 or not _sup_audio.exists():
                    raise RuntimeError(f"support master-audio extract failed: {(_acr.stderr or '')[-300:]}")
                # 2) word timestamps + phrase spans
                # v864 — release first, then measure, then refuse if too tight.
                _v864_release()
                _avail_mb, _rss_mb = _v864_mem()
                # v864.1 — `import os as _os864`, NOT bare `os`. _do_export_final
                # has a function-local `import os` further down, which makes `os`
                # local for the WHOLE function scope, so reading it here raised
                # UnboundLocalError and skipped the track ("cannot access local
                # variable 'os'"). Same workaround the function already uses at
                # its other local-import site.
                import os as _os864
                _min_mb = int(_os864.environ.get("SUPPORT_TRACK_MIN_AVAIL_MB", "600"))
                try:
                    import mem_guard as _mg865
                    _mg865.log("pre-whisper (support-track)")
                except Exception:
                    pass
                print(f"[Export][v864] pre-whisper mem: avail={_avail_mb}MB "
                      f"rss={_rss_mb}MB (need >={_min_mb}MB, None=unknown/dev)", flush=True)
                if _avail_mb is not None and _avail_mb < _min_mb:
                    # Skipping keeps the finished export. Loading anyway risks an
                    # OOM kill that destroys it. Bump the instance or lower
                    # SUPPORT_TRACK_MIN_AVAIL_MB to re-enable.
                    raise RuntimeError(
                        f"insufficient memory for support-track whisper load: "
                        f"avail={_avail_mb}MB < {_min_mb}MB — export kept, track skipped"
                    )
                # v864 — serialize: never let two exports hold a whisper model
                # at the same time on this instance.
                async with _V864_SUPPORT_LOCK:
                    _mw = await asyncio.to_thread(_tma, _sup_audio)
                _v864_release()
                _a2, _r2 = _v864_mem()
                print(f"[Export][v864] post-whisper mem: avail={_a2}MB rss={_r2}MB", flush=True)
                # v825.9 — hand the resolver each scene's spoken-line candidates
                # (Prompt A line + the Prompt-B reworded line) so a support is
                # placed INSIDE its owning line's master span, correct whether A
                # or B actually shipped. Without this a Prompt-B reword drops the
                # literal anchor word and the still lands wrong (or vanished
                # pre-v825.8).
                _scene_lines = []
                for _sc in (_parsed.get("scenes", []) or []):
                    _auth = " ".join(_sc.get("lines") or []).strip()
                    _cands = [_auth] if _auth else []
                    for _vp in (_sc.get("veo_prompts") or []):
                        _bl = (_vp or {}).get("prompt_b_line")
                        if _bl and _bl.strip():
                            _cands.append(_bl.strip())
                    _scene_lines.append({"authored": _auth, "candidates": _cands})
                _spans = _rss(_mw, _sup, _scene_lines if any(sl["authored"] for sl in _scene_lines) else None)
                # v866 — the 2026-07-23 kill happened somewhere after this point
                # with no further log line. Label every remaining stage so the
                # sampler attributes the spike.
                try:
                    import mem_guard as _mg866b
                    _mg866b.set_phase("support:fetch-stills")
                    _mg866b._sample_once(force=True, tag="after-span-resolve")
                except Exception:
                    _mg866b = None
                # 3) image_index -> approved still path (batch nodes named "... Scene N")
                _nodes = db.query(ImageNode).filter(ImageNode.batch_id == _sb.id).all()
                _idx_to_path = {}
                for _n in _nodes:
                    _m = _re2.search(r"Scene\s+(\d+)\s*$", _n.name or "")
                    if not _m or not _n.chosen_variant_id:
                        continue
                    _v = db.query(ImageVariant).filter(ImageVariant.id == _n.chosen_variant_id).first()
                    if _v and _v.image_path:
                        _abs = _iroot() / _v.image_path
                        if not _abs.exists():
                            _dl2(_v.image_path)  # rehydrate from R2 if evicted
                        if _abs.exists():
                            _idx_to_path[int(_m.group(1))] = str(_abs)
                # 4) assemble support clips (skip any still missing locally)
                _sup_clips = []
                for _sp_ in _spans:
                    if not _sp_:
                        continue
                    _p = _idx_to_path.get(_sp_["image_index"])
                    if not _p or not Path(_p).exists():
                        print(f"[Export][v825] no local still for image_{_sp_.get('image_index')} (path={_p})", flush=True)
                        continue
                    _sup_clips.append({**_sp_, "path": _p})
                if _sup_clips:
                    _mdur = _gd(_fpj(output_path))
                    # v825.4 — ONE support track PER ASPECT RATIO (operator wants
                    # the 16:9 overlays in a separate video from the 1:1 / 9:16
                    # ones, each at its native canvas so it drops cleanly in post
                    # with no letterboxing). Group the stills by their image's
                    # aspect_ratio; every track is full master length so they all
                    # align on the same timeline. Each uploaded to R2 (else the
                    # file lives only on Render's ephemeral disk -> not served).
                    from collections import defaultdict as _dd
                    _ar_by_idx = {i["image_index"]: (i.get("aspect_ratio") or "9:16")
                                  for i in _parsed.get("images", [])}
                    _canvas = {"16:9": (1920, 1080), "9:16": (1080, 1920),
                               "1:1": (1080, 1080), "4:3": (1440, 1080), "3:4": (1080, 1440)}
                    _groups = _dd(list)
                    for _c in _sup_clips:
                        _groups[_ar_by_idx.get(_c["image_index"], "9:16")].append(_c)
                    # v825.6 — stamp each support track with the speaker export's
                    # id (YYYYMMDD_HHMMSS_hash) so it's unique per export (not
                    # overwritten by the next export) and the UI can pair it with
                    # its speaker + broll of the same export.
                    _stem = output_filename.replace('final_export_', '').replace('.mp4', '')
                    _tracks = []
                    for _ar, _grp in _groups.items():
                        _w, _h = _canvas.get(_ar, (1080, 1920))
                        _fn = f"support_track_{_ar.replace(':', 'x')}_{_stem}.mp4"
                        _sup_out = output_dir / _fn
                        if _mg866b is not None:
                            _mg866b.set_phase(f"support:ffmpeg {_ar} ({len(_grp)} stills @{_w}x{_h})")
                            _mg866b._sample_once(force=True, tag=f"before-track-{_ar}")
                        # v867 — HARD SAFETY GATE. The support track is a
                        # best-effort post-production convenience; it must NEVER
                        # be able to OOM-kill a finished export. If the CONTAINER
                        # (cgroup, not host) is already tight right before we
                        # spawn ffmpeg, skip this track and keep the export. The
                        # operator still has the stills in the image job. This
                        # reads the correct number (v865) at the correct point
                        # (immediately pre-ffmpeg), unlike the v864 pre-whisper
                        # check which measured the wrong thing at the wrong step.
                        try:
                            import mem_guard as _mgg
                            import os as _osg867
                            _need = int(_osg867.environ.get("SUPPORT_TRACK_MIN_AVAIL_MB", "700"))
                            _snap = _mgg.snapshot()
                            if _snap["source"] == "cgroup" and _snap["avail_mb"] is not None \
                                    and _snap["avail_mb"] < _need:
                                print(f"[Export][v867] SKIP support-track {_ar}: "
                                      f"avail={_snap['avail_mb']}MB < {_need}MB "
                                      f"(used={_snap['used_mb']}MB/{_snap['limit_mb']}MB) — "
                                      f"export kept, still PNGs available in the image job",
                                      flush=True)
                                continue
                        except Exception:
                            pass
                        _est(_grp, _mdur, _sup_out, width=_w, height=_h)
                        if _mg866b is not None:
                            _mg866b._sample_once(force=True, tag=f"after-track-{_ar}")
                        try:
                            if storage is not None:
                                await asyncio.to_thread(storage.upload_file, str(_sup_out),
                                                        f"jobs/{job_id}/outputs/{_fn}", 'video/mp4')
                                print(f"[Export][v825] {_fn} uploaded to R2", flush=True)  # TEMP DIAG
                        except Exception as _st_up_e:
                            print(f"[Export][v825] {_fn} R2 upload failed (non-fatal): {_st_up_e}", flush=True)
                        _tracks.append({"aspect_ratio": _ar, "filename": _fn,
                                        "url": f"/api/jobs/{job_id}/outputs/{_fn}", "stills": len(_grp)})
                        print(f"[Export][v825] {_fn} -> {len(_grp)} stills @ {_w}x{_h}", flush=True)  # TEMP DIAG
                    support_track_info = {"support_tracks": _tracks}
                else:
                    print("[Export][v825] no resolvable support stills; skipping track", flush=True)
        except Exception as _sup_e:
            print(f"[Export][v825] support-track skipped (non-fatal): {_sup_e}", flush=True)
        try:
            import mem_guard as _mg866c
            _mg866c._sample_once(force=True, tag="export-done")
            _mg866c.set_phase("idle")
        except Exception:
            pass

        # v890 — surface what beat alignment actually did (or why it did not).
        # Same lesson as v888.2's music_applied: an export that silently fell
        # back to the authored timings must be distinguishable from one that
        # aligned, without anyone having to watch the video.
        try:
            if isinstance(stats, dict):
                stats.update(_beat_report)
        except Exception:
            pass

        return {
            "success": True,
            "filename": output_filename,
            "download_url": f"/api/jobs/{job_id}/outputs/{output_filename}",
            "stats": stats,
            **audio_info,
            **support_track_info,
        }
        
    except Exception as e:
        import traceback
        print(f"[Export] ERROR: {str(e)}")
        print(f"[Export] Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
    finally:
        # v945.3 — the muxed-copy directory is cleaned by _do_export_final,
        # which owns it for the whole call. It used to be cleaned here, which
        # covered only the exits BELOW its creation point; the preflight
        # raises above this try never reached it.
        # v872 — HAND THE MEMORY BACK, on the failure path too.
        #
        # Before this, an export left its Whisper models, its torch/soundfile
        # buffers and glibc's un-returned heap resident for the life of the
        # container: boot rss=173MB, post-export rss~1.6GB, and the NEXT export
        # started from there. That ratchet is what finally hit the 2GB cgroup
        # ceiling on job be09f595. Releasing here makes each export start from
        # roughly the same baseline instead of the previous export's high-water
        # mark, and the trim is what actually returns the pages to the kernel.
        try:
            import video_processor as _vp872
            _vp872.release_cached_models("export-end")
        except Exception as _rel_e:
            print(f"[Export/v872] model release failed (non-fatal): {_rel_e}", flush=True)
        try:
            import mem_guard as _mg872
            _mg872.trim("export-end")
            _mg872.set_phase("idle")
            _mg872._sample_once(force=True, tag="export-released")
        except Exception:
            pass


@app.post("/api/jobs/{job_id}/export-final")
async def export_final_video(
    job_id: str,
    settings: ExportSettings,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v850 — QUEUE an export. Returns immediately (202); the work runs
    detached so a Render deploy can't kill it. Poll /export-status."""
    job = get_user_job(db, job_id, current_user)  # 404/403 if not the caller's

    # v947 — the build's declared export_* finishing supplies DEFAULTS; a field
    # the caller explicitly sent always wins (rev-459 shape, model_fields_set).
    from auto_finish import derive_export_defaults
    _spec = _job_finishing_spec(job)
    if _spec and _spec.get("export"):
        settings = ExportSettings(**derive_export_defaults(
            settings.model_dump(), _spec, settings.model_fields_set))
        print(f"[Finishing/v947] job={job_id[:8]} export derive applied "
              f"({sorted(_spec['export'])})", flush=True)

    # Idempotent: a second click (or the browser retrying after a dropped
    # connection) joins the export already in flight instead of starting a
    # duplicate 15-minute ffmpeg run. v855: no rescue-spawn needed on the join
    # path — the dispatcher picks up ANY queued run within DISPATCH_INTERVAL_S,
    # including one a dead container left behind, and it is the only spawn path
    # (it is what enforces the concurrency cap).
    run, created = _queue_export_run(db, job, settings, current_user.id)
    return JSONResponse(status_code=202, content=run.to_dict())


@app.get("/api/jobs/{job_id}/export-status")
async def export_status(
    job_id: str,
    export_id: str = None,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v850 — poll target for the frontend. Returns the named run, or the most
    recent one for this job. `result` carries the exact payload the old
    synchronous endpoint used to return (filename / download_url / stats /
    audio / support_tracks)."""
    from models import ExportRun

    get_user_job(db, job_id, current_user)

    q = db.query(ExportRun).filter(ExportRun.job_id == job_id)
    if export_id:
        q = q.filter(ExportRun.id == export_id)
    run = q.order_by(ExportRun.created_at.desc()).first()
    if not run:
        raise HTTPException(status_code=404, detail="No export has been requested for this job")
    return run.to_dict()


# ============ Auto-edit (CapCut-pass) — queue here, render on a local worker ============
# The render needs OpenCV + a headless browser and several minutes of CPU; this
# box has 1 CPU / 2GB. So the server only queues rows and stores results, and a
# worker on the operator's PC claims them. autoedit_pipeline is imported INSIDE
# each function on purpose — its heavy deps live inside function bodies, and
# keeping the import local means a missing optional dep can never break boot.

AUTOEDIT_PLACEMENTS = ("dynamic", "constant")
# ~10x a real auto-edit output (they run ~50MB). The write streams to disk in
# 1MB chunks so memory is never the worry — DISK is, on a 2GB box.
AUTOEDIT_MAX_UPLOAD_BYTES = 512 * 1024 * 1024


def _autoedit_valid_templates():
    from autoedit_pipeline import local_styles, BUILTIN_TEMPLATES
    return list(local_styles()) + list(BUILTIN_TEMPLATES)


def _autoedit_validate(template: str, placement: str):
    """Reject a bad template/placement HERE. Otherwise the row queues fine and
    only blows up in the worker, after a download and minutes of rendering."""
    valid = _autoedit_valid_templates()
    if template not in valid:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown template '{template}'. Valid templates: {', '.join(valid)}")
    if placement not in AUTOEDIT_PLACEMENTS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown placement '{placement}'. Valid placements: "
                   f"{', '.join(AUTOEDIT_PLACEMENTS)}")


# AutoEditRequest moved to finishing_models.py (v947) — imported at top of file.


def derive_autoedit_defaults(req_dict, spec, request_was_explicit):
    """v944 — fold the build's declared FINISHING into an auto-edit request.

    `req_dict` is what the caller asked for, `spec` is the job's declared
    finishing (or None), and `request_was_explicit` is the set of fields the
    caller actually sent — pydantic's `model_fields_set`, so a value that only
    equals the default is NOT treated as a choice.

    The rev-459 inheritance shape: the declaration supplies DEFAULTS, an
    explicit request always wins. Returns a NEW dict; the caller's is not
    touched.

    `spec is None` (a build that declared no finishing, which is every build
    imported before this rule) returns the request unchanged apart from
    `overlay_spec: None` — that is the regression contract, in one line.
    """
    out = dict(req_dict or {})
    if not spec:
        # The regression contract protects jobs that declared nothing — it must
        # not eat an overlay the caller EXPLICITLY sent (the pilot re-finish:
        # a pre-v944 job with the spec passed in the request). Measured live on
        # run ef707c39: the explicit overlay came back None from this line.
        if "overlay_spec" not in request_was_explicit:
            out["overlay_spec"] = None
        else:
            out.setdefault("overlay_spec", None)
        return out

    captions = str(spec.get("captions") or "none").lower()
    if "captions_enabled" not in request_was_explicit:
        out["captions_enabled"] = captions != "none"
    if captions != "none" and "template" not in request_was_explicit:
        out["template"] = captions

    if "overlay_spec" in request_was_explicit:
        return out
    out["overlay_spec"] = spec if spec.get("overlay") == "readcaption" else None
    return out


def _queue_autoedit_impl(db, job, req: AutoEditRequest, user_id):
    """The whole queue-an-autoedit body (hook-layout inheritance, v944 derive,
    normalize_repairs, done-export precondition, can_queue, row creation).
    Shared by the endpoint and the v947 auto-finish chain. Raises HTTPException
    exactly as the endpoint did; the auto-chain caller catches. Returns the
    AutoEditRun."""
    from models import AutoEditRun, ExportRun
    from autoedit_queue import can_queue
    import uuid as _uuid

    job_id = job.id
    placement = "constant" if req.offset is not None else req.placement
    if req.offset is not None and not -0.45 <= req.offset <= 0.45:
        raise HTTPException(
            status_code=400,
            detail="Caption offset must be between -0.45 and 0.45",
        )
    from autoedit_qc import normalize_repairs
    # Settings STICK to the job (operator 2026-08-25, after a default-settings
    # re-queue silently dropped an agreed hook layout): when the request names
    # neither hook field, inherit both from the job's most recent run, so a
    # re-render keeps what was decided for this job. Explicit values —
    # including hook_corner=0, the explicit OFF — always win and are what a
    # later run then inherits.
    hook_corner_req, hook_bg_req = req.hook_corner, req.hook_bg
    if hook_corner_req is None and hook_bg_req is None:
        # Deliberately NO state filter (reviewer asked): repairs capture the
        # operator's CHOICE at queue time, not the render's outcome. A failed
        # or cancelled run still carries the settings the operator wanted —
        # filtering to done-only would lose them on a retry, which is the
        # exact silent-drop this inheritance exists to prevent.
        prev = db.query(AutoEditRun).filter(
            AutoEditRun.job_id == job_id,
        ).order_by(AutoEditRun.created_at.desc()).first()
        if prev is not None and prev.repair_json:
            try:
                prev_rep = json.loads(prev.repair_json)
                hook_corner_req = prev_rep.get("hook_corner")
                hook_bg_req = prev_rep.get("hook_bg")
                if hook_corner_req is not None or hook_bg_req is not None:
                    print(f"[AutoEdit] job={job_id[:8]} inheriting hook layout "
                          f"from run {prev.id[:8]}: corner={hook_corner_req} "
                          f"bg={hook_bg_req}", flush=True)
            except (ValueError, TypeError):
                pass

    # v944 — the BUILD's declared finishing supplies the defaults. This is the
    # whole root cause of the de7f9331 finish: the auto-edit ran with
    # template=korella / captions_enabled=True because nothing in the job said
    # otherwise, and the build had agreed on an overlay and no captions.
    # Same inheritance shape as the hook layout above: declaration = default,
    # an explicitly-sent field always wins. `model_fields_set` is what makes
    # "explicit" real — a value that merely equals the default is not a choice.
    _v944_spec = None
    if getattr(job, "finishing_spec", None):
        try:
            _v944_spec = json.loads(job.finishing_spec)
        except (ValueError, TypeError):
            # A corrupt stored value degrades to "declared nothing" rather than
            # blocking a re-finish. It was validated at import; if it is broken
            # now, the import is the thing to fix.
            print(f"[v944] job={job_id[:8]} finishing_spec is not valid JSON — ignored",
                  flush=True)
    _v944 = derive_autoedit_defaults(
        {
            "template": req.template,
            "captions_enabled": req.captions_enabled,
            "overlay_spec": req.overlay_spec,
        },
        _v944_spec,
        request_was_explicit=req.model_fields_set,
    )
    template = _v944["template"]
    if _v944_spec is not None:
        print(f"[v944] job={job_id[:8]} finishing declared {_v944_spec} -> "
              f"template={template} captions={_v944['captions_enabled']} "
              f"overlay={'readcaption' if _v944['overlay_spec'] else 'none'}",
              flush=True)
    _autoedit_validate(template, placement)

    try:
        repairs = normalize_repairs({
            "trim_start_s": req.trim_start_s,
            "trim_end_s": req.trim_end_s,
            "pip_enabled": req.pip_enabled,
            "captions_enabled": _v944["captions_enabled"],
            "chroma_similarity": req.chroma_similarity,
            "chroma_blend": req.chroma_blend,
            "music_filename": req.music_filename,
            "music_db": req.music_db,
            "audio_enhance": req.audio_enhance,
            "hook_corner": hook_corner_req,
            "hook_bg": hook_bg_req,
            "overlay_spec": _v944["overlay_spec"],
        })
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # Serializes concurrent queue attempts (the v947 chain, an operator click,
    # a sweeper double-completion): the can_queue read below is check-then-act,
    # and the job row is the mutex — same pattern as _maybe_auto_finish_export.
    # Postgres emits FOR UPDATE; sqlite ignores it. Deliberately NOT rebinding
    # `job`: this is taken for the lock only. Released by the commit on the
    # success path; on a raise there is NO explicit rollback anywhere — the
    # release is the session close (endpoint: get_db_session teardown; chain:
    # the get_db context exit), both bounded by the same request/call.
    # NOTE: the mutex itself has no test coverage — sqlite ignores FOR UPDATE,
    # so the passing double-fire test proves can_queue, not this lock.
    db.query(Job).filter(Job.id == job.id).with_for_update().first()

    exp = db.query(ExportRun).filter(
        ExportRun.job_id == job_id, ExportRun.state == "done"
    ).order_by(ExportRun.created_at.desc()).first()
    if not exp:
        raise HTTPException(
            status_code=409,
            detail="Export the final video first — auto-edit runs on the export")

    states = [r.state for r in db.query(AutoEditRun).filter(AutoEditRun.job_id == job_id)]
    if not can_queue(states):
        raise HTTPException(
            status_code=409,
            detail="An auto-edit is already queued or running for this job")

    # A NULL user_id can never match the worker's claim filter, so the row would
    # sit queued forever with nothing anywhere reporting a problem. That is a
    # server-side invariant break, not the caller's fault — fail loud, 500.
    if not user_id:
        raise HTTPException(
            status_code=500,
            detail="Cannot queue auto-edit: the signed-in user has no id, so no "
                   "worker could ever claim the run")

    run = AutoEditRun(
        id=str(_uuid.uuid4()), job_id=job_id, user_id=user_id,
        template=template, placement=placement, offset=req.offset,
        repair_json=json.dumps(repairs, sort_keys=True),
    )
    db.add(run)
    db.commit()
    print(f"[AutoEdit/v937 TEMP] queued {run.id} job={job_id} "
          f"template={template} repairs={repairs}", flush=True)
    return run


@app.post("/api/jobs/{job_id}/autoedit")
async def queue_autoedit(
    job_id: str,
    req: AutoEditRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Queue an auto-edit for this job. Needs a finished export to work on."""
    job = get_user_job(db, job_id, current_user)
    run = _queue_autoedit_impl(db, job, req, getattr(current_user, "id", None))
    return run.to_dict()


@app.get("/api/jobs/{job_id}/autoedit-status")
async def autoedit_status(
    job_id: str,
    autoedit_id: str = None,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Poll target for the frontend: the named run, or the newest for this job."""
    from models import AutoEditRun

    get_user_job(db, job_id, current_user)

    q = db.query(AutoEditRun).filter(AutoEditRun.job_id == job_id)
    if autoedit_id:
        q = q.filter(AutoEditRun.id == autoedit_id)
    run = q.order_by(AutoEditRun.created_at.desc()).first()
    if not run:
        raise HTTPException(status_code=404, detail="No auto-edit for this job")
    # v938.2 — this is a POLL. It carried no cache headers, so a browser was
    # free to serve a cached copy and the progress bar sat frozen on whatever
    # the first poll returned. Say explicitly that it must never be cached.
    return JSONResponse(content=run.to_dict(),
                        headers={"Cache-Control": "no-store, max-age=0"})


@app.post("/api/jobs/{job_id}/autoedit/{autoedit_id}/cancel")
async def cancel_autoedit(
    job_id: str,
    autoedit_id: str,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Cancel a run that has not been claimed by the local worker."""
    from models import AutoEditRun
    from datetime import datetime as _dt

    get_user_job(db, job_id, current_user)
    run = db.query(AutoEditRun).filter(
        AutoEditRun.id == autoedit_id,
        AutoEditRun.job_id == job_id,
    ).first()
    if not run:
        raise HTTPException(status_code=404, detail="No such auto-edit run")
    if run.state != "queued":
        raise HTTPException(status_code=409, detail="Only a queued auto-edit can be cancelled")
    run.state = "failed"
    run.error = "Cancelled before the local worker started"
    run.finished_at = _dt.utcnow()
    db.commit()
    return run.to_dict()


class FinishingUpdate(BaseModel):
    markdown: str


@app.post("/api/jobs/{job_id}/finishing")
async def update_job_finishing(
    job_id: str,
    req: FinishingUpdate,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v947 — re-parse a build markdown's ## Finishing onto an EXISTING job.

    The import->promote path only carries the spec for jobs promoted after
    their build declared it; this is the bridge for every job promoted
    before. Same parser as import (fail-closed), so a bad section 400s here
    exactly as it would die at import. An absent section CLEARS the stored
    spec (parity with re-import semantics: removing the section must not
    leave the job on the old finish forever).
    """
    from image_platform import parse_finishing_section
    job = get_user_job(db, job_id, current_user)  # 404/403 if not the caller's
    try:
        spec = parse_finishing_section(req.markdown or "")
    except ValueError as exc:
        # "Parse error:" prefix = the send_to_platform CLI classifies this as
        # EXIT_PARSE (2), same as every other parse failure it can receive.
        raise HTTPException(status_code=400, detail=f"Parse error: {exc}")
    job.finishing_spec = json.dumps(spec) if spec else None
    db.commit()
    print(f"[Finishing/v947] job={job_id[:8]} finishing_spec updated via API: "
          f"{spec if spec else 'CLEARED (no section)'}", flush=True)

    # v947.1 — the already-approved case (operator 2026-08-27: "do it"). A job
    # whose clips are ALL approved will never see another approve_clip, so a
    # spec stored now would be a dead letter. Fire the same trigger the
    # approval path uses; every one of its gates applies (auto_finish on,
    # every clip approved, idempotent join, job-row lock), so on any other
    # job state this is a no-op. The spec is already committed above — a
    # trigger error rolls back only the trigger's own work.
    auto_finish_fired = None
    try:
        fired = _maybe_auto_finish_export(db, job)
        if fired:
            auto_finish_fired = {"export_id": fired[0], "created": fired[1]}
    except Exception as _af:
        db.rollback()
        print(f"[AutoFinish] job={job_id[:8]} update-finishing trigger error "
              f"(spec stored): {_af}", flush=True)
    return {"job_id": job_id, "finishing_spec": spec,
            "auto_finish_fired": auto_finish_fired}


@app.get("/api/autoedit/templates")
async def autoedit_templates():
    """Style menu for the UI. Local templates + pycaps builtins."""
    from autoedit_pipeline import local_styles, BUILTIN_TEMPLATES
    return {"default": "korella",
            "local": local_styles(), "builtin": BUILTIN_TEMPLATES}


# The four WORKER-side auto-edit endpoints live further down, right after
# verify_user_worker_token is defined — `Depends(...)` reads that name while the
# module is still executing, so they cannot sit above its def.


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

def _mem_probe():
    """v890.3 — cgroup memory for the health probe. Never raises."""
    try:
        import mem_guard as _mg
        s = _mg.snapshot()
        return {"source": s.get("source"), "limit_mb": s.get("limit_mb"),
                "used_mb": s.get("used_mb"), "avail_mb": s.get("avail_mb"),
                "rss_mb": s.get("rss_mb")}
    except Exception as e:
        return {"error": str(e)[:120]}


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
        "storage": storage_status,
        # v890.3 — container memory on the unauthenticated health probe, so a
        # memory problem can be WATCHED from outside without dashboard access,
        # the same reason render_commit is here. Beat analysis adds a ~150MB
        # windowed spike and the box is 2GB with an OOM history; "it seemed
        # fine" is not evidence. Never raises: a health probe that dies on its
        # own diagnostics is worse than one without them.
        "memory": _mem_probe(),
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


def _v812_audio_anchor_fallback(db, clip, rejected_key):
    """v812 — audio-twin anchor auto-swap on image policy reject.

    An audio_pair clip's visual is DISCARDED at export (only its voice track
    is used), so when Flow rejects its anchor image there is no reason to
    stop and ask the operator for a replacement — ANY image of the main
    character lip-syncs fine. Fallback chain (operator 2026-07-02):
      1. other variants of the SAME anchor ImageNode (same image batch)
      2. chosen variants of OTHER nodes character-linked to the same
         character upload (any other main-character image)
      3. LAST RESORT: the character reference upload itself (always exists,
         already passed policy as an upload)
    Progress across rejections is encoded statelessly in the frame name
    (anchor_fb<N>_...): each swap uploads the next candidate as N+1.

    Returns the new jobs/<job>/frames/... R2 key, or None (no candidates /
    chain exhausted / storage off) — caller falls through to the normal
    FAILED + replace-image card.
    """
    try:
        if (clip.clip_role or '') != 'audio_pair':
            return None
        from backends.storage import get_storage, is_storage_configured
        if not is_storage_configured():
            return None
        import re as _re
        import image_platform as _ip

        anchor_node_id = clip.voiceover_anchor_image_node_id
        if not anchor_node_id:
            return None
        anchor_node = db.query(_ip.ImageNode).filter(
            _ip.ImageNode.id == anchor_node_id).first()
        if anchor_node is None:
            return None

        candidates = []
        # (1) same-batch sibling variants of the anchor image
        for v in db.query(_ip.ImageVariant).filter(
                _ip.ImageVariant.node_id == anchor_node_id
        ).order_by(_ip.ImageVariant.variant_index).all():
            if v.id != anchor_node.chosen_variant_id:
                candidates.append(v)
        # (2) any other main-character image + (3) the character upload itself
        char_edge = db.query(_ip.ImageEdge).filter(
            _ip.ImageEdge.child_node_id == anchor_node_id,
            _ip.ImageEdge.kind == 'character').first()
        if char_edge:
            # order_by(id) — the chain index (anchor_fbN) is parsed across
            # separate violation calls, so candidate order MUST be stable
            # even if new edges appear in between (v812.1).
            for e in db.query(_ip.ImageEdge).filter(
                    _ip.ImageEdge.parent_node_id == char_edge.parent_node_id,
                    _ip.ImageEdge.kind == 'character',
                    _ip.ImageEdge.child_node_id != anchor_node_id
            ).order_by(_ip.ImageEdge.id).all():
                n = db.query(_ip.ImageNode).filter(
                    _ip.ImageNode.id == e.child_node_id).first()
                if n and n.chosen_variant_id:
                    v = db.query(_ip.ImageVariant).filter(
                        _ip.ImageVariant.id == n.chosen_variant_id).first()
                    if v:
                        candidates.append(v)
            up = db.query(_ip.ImageNode).filter(
                _ip.ImageNode.id == char_edge.parent_node_id).first()
            if up:
                v = None
                if up.chosen_variant_id:
                    v = db.query(_ip.ImageVariant).filter(
                        _ip.ImageVariant.id == up.chosen_variant_id).first()
                if v is None:
                    v = db.query(_ip.ImageVariant).filter(
                        _ip.ImageVariant.node_id == up.id).first()
                if v:
                    candidates.append(v)
        if not candidates:
            return None

        # Where are we in the chain? The rejected frame's name says.
        # split('/') not os.path.basename — R2 keys always use forward
        # slashes regardless of host OS (v812.1).
        cur = (rejected_key or clip.start_frame or '').split('/')[-1]
        m = _re.match(r'anchor_fb(\d+)_', cur)
        next_idx = (int(m.group(1)) + 1) if m else 0
        if next_idx >= len(candidates):
            print(f"[v812] clip {clip.id}: fallback chain exhausted "
                  f"({len(candidates)} candidates) — falling through to replace-image card", flush=True)
            return None

        pick = candidates[next_idx]
        local = _ip.images_root() / pick.image_path
        if not local.exists():
            _ip._storage_download_to_local(pick.image_path)
        if not local.exists():
            print(f"[v812] clip {clip.id}: candidate variant {pick.id} file missing "
                  f"locally + no R2 restore — skipping swap", flush=True)
            return None

        frame_name = f"anchor_fb{next_idx}_v{pick.id}{local.suffix or '.png'}"
        new_key = get_storage().upload_job_frame(clip.job_id, frame_name, local)
        print(f"[v812] clip {clip.id}: anchor auto-swap #{next_idx} → variant {pick.id} "
              f"({new_key})", flush=True)
        return new_key
    except Exception as _e:
        import traceback
        print(f"[v812] anchor-fallback lookup failed for clip {getattr(clip, 'id', '?')}: {_e}", flush=True)
        traceback.print_exc()
        return None


def _v812_apply_swap(db, clip, rejected_key, new_key):
    """v812 — apply the anchor swap: new frame on start_frame, rejected key
    kept for audit, clip requeued for redo (same gate the replace-image card
    uses). Returns the endpoint response dict."""
    if rejected_key:
        clip.replacement_start_frame = rejected_key
    clip.start_frame = new_key
    clip.status = ClipStatus.FLOW_REDO_QUEUED.value

    # v899.5 — PRESERVE WHY BEFORE CLEARING.
    # error_code/error_message have to be cleared so the clip re-enters the
    # queue clean, but nulling them used to destroy the ONLY record of why the
    # clip was redone. Asked "why were clips #4 and #12 redone?" on job
    # f58e833f (2026-08-18) the answer was unrecoverable: error_code empty,
    # error_message empty, and the reason existed only in a worker console that
    # had already scrolled away. redo_reason is free text and survives the
    # requeue, so the audit trail lives there.
    _why = (clip.error_message or clip.error_code or "").strip()
    if _why:
        _stamp = f"auto-swap ({_why})"
        if rejected_key:
            _stamp += f" rejected={str(rejected_key).split('/')[-1]}"
        clip.redo_reason = (
            f"{clip.redo_reason} | {_stamp}" if clip.redo_reason else _stamp
        )[:1000]
        print(f"[v899.5] clip {clip.id}: preserved redo reason -> {_stamp}", flush=True)

    clip.error_code = None
    clip.error_message = None
    clip.claimed_by_worker = None
    clip.claimed_at = None
    db.commit()
    # v812.1 — the swap is already committed above; a job-log hiccup must not
    # turn the response into a 500 (the worker's error fallback would mark the
    # clip failed and clobber the requeue).
    try:
        add_job_log(
            db, clip.job_id,
            f"Clip {clip.clip_index + 1}: audio-twin anchor rejected — auto-swapped to "
            f"fallback image ({new_key.split('/')[-1]}) and requeued (v812)",
            "WARNING", "policy",
        )
        db.commit()
    except Exception as _log_err:
        print(f"[v812] job-log write failed (swap already committed, non-fatal): {_log_err}", flush=True)
        try:
            db.rollback()
        except Exception:
            pass
    return {
        "ok": True,
        "clip_id": clip.id,
        "auto_swapped": True,   # v812
        "new_start_frame": new_key,
        "rejected_image_key": rejected_key or None,
        "cascaded_sibling_count": 0,
    }


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

        # v812 — audio-twin anchor auto-swap: an audio_pair clip's visual is
        # discarded at export, so swap in the next fallback persona image and
        # requeue instead of failing + waiting for a manual replacement.
        _v812_key = _v812_audio_anchor_fallback(db, clip, rejected_key)
        if _v812_key:
            return _v812_apply_swap(db, clip, rejected_key, _v812_key)

        # v815 — prominent-people / celebrity auto-retry branch. Scoped to
        # this reason ONLY; generic content-policy keeps the manual card path.
        is_prominent = (request.error_reason or "").upper()
        is_prominent = ("PROMINENT" in is_prominent) or ("CELEBRITY" in is_prominent)
        if is_prominent:
            clip.error_code = "PROMINENT_PEOPLE_FILTER"
            clip.error_message = request.detail or "Rejected (prominent people). Auto-retry in progress."
            if rejected_key:
                clip.replacement_start_frame = rejected_key
            # v815 — NO commit here: let the outcome commit atomically so a
            # retry failure can't leave the clip stamped-but-not-swapped.
            applied = None
            try:
                applied = _auto_image_retry(db, clip, rejected_key)
            except Exception as _ar_err:
                import traceback
                print(f"[v815] auto-image-retry FAILED for clip {clip_id}: "
                      f"{type(_ar_err).__name__}: {_ar_err}", flush=True)
                traceback.print_exc()
                db.rollback()
                applied = None
            if applied:
                return {"ok": True, "clip_id": clip_id, "auto_retry": applied}
            # disabled / exhausted / errored -> manual card, single atomic
            # commit. Re-stamp: db.rollback() discarded the in-memory fields.
            clip.error_code = "PROMINENT_PEOPLE_FILTER"
            clip.error_message = request.detail or "Rejected (prominent people). Upload a replacement to retry."
            if rejected_key:
                clip.replacement_start_frame = rejected_key
            clip.status = ClipStatus.FAILED.value
            db.commit()
            return {"ok": True, "clip_id": clip_id, "auto_retry": None,
                    "rejected_image_key": rejected_key or None}
        # (existing generic CONTENT_POLICY_VIOLATION path continues unchanged below)

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
                    # v812 — never pre-fail an audio twin: it self-heals via
                    # its OWN violation report (anchor auto-swap + requeue).
                    if (sib.clip_role or '') == 'audio_pair':
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

    # v921 — recover jobs STRANDED at 'processing' by a worker that died.
    # Mirror of the user-worker endpoint; see the full rationale there. Keyed on
    # updated_at (touched by every clip report) rather than claimed_at, because
    # a real multi-clip job outlives the 10-minute claim window and releasing on
    # claim age would double-submit a live worker's clips.
    stranded_cutoff = datetime.utcnow() - timedelta(minutes=30)
    stranded_jobs = db.query(Job).filter(
        Job.backend == 'flow',
        Job.status == 'processing',
        Job.updated_at < stranded_cutoff
    ).all()

    for sj in stranded_jobs:
        print(f"[Worker] v921 releasing STRANDED job {sj.id[:8]} "
              f"(processing, no clip activity since {sj.updated_at}, "
              f"was claimed by {sj.claimed_by_worker})", flush=True)
        sj.claimed_by_worker = None
        sj.claimed_at = None
        sj.status = 'pending'
    if stranded_jobs:
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

    # v825 — BULLETPROOF stuck-'processing' reaper (backstop for v824).
    # get_pending_job only serves 'pending'/'queued_for_flow', so a Flow job
    # left at status='processing' with completed_clips < total_clips and NO
    # further progress is NEVER re-served → "one account failed and the whole
    # thing got dropped". v824 recovers the common case (account self-resumes
    # its slice across a supervisor restart); this reaper is the last-resort net
    # for total death the worker can't recover from itself — whole worker
    # process killed, machine reboot, or an account thread dead past
    # MAX_WORKER_RESTARTS. It runs server-side on every poll, independent of any
    # crashed worker's in-memory state, so recovery is driven by whichever
    # worker polls next (this one, a restarted one, or the VPS worker).
    #
    # Progress signal = Job.updated_at (Clip has no updated_at column, v487).
    # updated_at bumps whenever completed_clips changes, so a live job that is
    # actually completing clips keeps it fresh. Two-tier stale gate avoids
    # false-resetting a live-but-slow job mid-generation:
    #   owner worker OFFLINE → 15 min (nothing is happening, recover fast)
    #   owner worker ONLINE  → 45 min (give the live worker lots of room)
    # Reset only IN-FLIGHT clips (non-terminal) back to 'pending'; completed /
    # failed / skipped / redo-queued clips are preserved, so on re-serve the
    # worker re-submits only unfinished clips and the job reaches a terminal
    # state (no reap loop; worker-side v788 cap fails truly-broken clips).
    try:
        _now = datetime.utcnow()
        _STUCK_OFFLINE_MIN = 15
        _STUCK_ONLINE_MIN = 45
        _TERMINAL_CLIP = {
            'completed', 'approved', 'failed', 'skipped',
            'redo_queued', 'flow_redo_queued',
        }
        _stuck_jobs = db.query(Job).filter(
            Job.backend == 'flow',
            Job.status == 'processing',
            Job.total_clips.isnot(None),
            Job.completed_clips.isnot(None),
            Job.completed_clips < Job.total_clips,
        ).all()
        _reaped = 0
        for _sj in _stuck_jobs:
            _owner_online = _sj.user_id in active_user_ids
            _limit = _STUCK_ONLINE_MIN if _owner_online else _STUCK_OFFLINE_MIN
            _last = _sj.updated_at or _sj.created_at
            if not _last or (_now - _last) < timedelta(minutes=_limit):
                continue
            _reset_clips = 0
            for _c in db.query(Clip).filter(Clip.job_id == _sj.id).all():
                if (_c.status or '').lower() not in _TERMINAL_CLIP:
                    _c.status = 'pending'
                    _c.claimed_by_worker = None
                    _c.claimed_at = None
                    _reset_clips += 1
            _sj.status = 'pending'
            _sj.claimed_by_worker = None
            _sj.claimed_at = None
            _sj.updated_at = _now  # bump so it isn't instantly re-reaped + refreshes the 24h window
            _reaped += 1
            print(f"[Worker] ⛑ v825 reaped stuck 'processing' job {_sj.id[:8]} "
                  f"(no progress {int((_now - _last).total_seconds() // 60)}min, "
                  f"owner_online={_owner_online}, {_sj.completed_clips}/{_sj.total_clips} done, "
                  f"{_reset_clips} in-flight clip(s) reset) → pending for re-dispatch", flush=True)
        if _reaped:
            db.commit()
    except Exception as _reap_err:
        print(f"[Worker] v825 reaper error (non-fatal): {_reap_err}", flush=True)
        db.rollback()

    # Build query for available jobs
    # Either: unclaimed, OR claimed by this same worker
    # Exclude any jobs the worker is already processing
    _age_cutoff = job_age_cutoff()
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

        if _age_cutoff is not None:
            query = query.filter(Job.created_at >= _age_cutoff)

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

        if _age_cutoff is not None:
            query = query.filter(Job.created_at >= _age_cutoff)

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
            "prompt_b": clip.prompt_text_b,  # v805 — policy-fallback prompt (voice-only)
            "error_message": clip.error_message,  # v849 — durable Prompt-B requeue marker (restart-safe re-derivation in process_redo_clip)
            "start_frame_key": start_frame_key,  # R2 key for frame
            "end_frame_key": end_frame_key,
            "status": clip.status,
            # Use proxy URLs instead of direct R2 presigned URLs
            "start_frame_url": f"{base_url}/api/local-worker/frames/{job.id}/{start_filename}" if start_filename else None,
            "end_frame_url": f"{base_url}/api/local-worker/frames/{job.id}/{end_filename}" if end_filename else None,
            # Storyboard/Scene mode fields for continue mode support
            "clip_mode": clip.clip_mode or "fresh",
            "scene_index": clip.scene_index or 0,
            # v861 — per-clip render duration (4|6|8|10). NULL → the worker
            # falls back to the job-level duration (legacy / manual jobs).
            "veo_render_duration_s": clip.veo_render_duration_s,
        }

        # v943 — charswap keys. Added only when the clip really is a swap, so a
        # legacy clip's payload keeps the exact shape it had before.
        clip_data = _v943_maybe_charswap(clip_data, clip, base_url, "local-worker")

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
    # Job.created_at is immutable, unlike updated_at above which the redo path
    # and the startup backfill both bump — that is why an ancient job could keep
    # refreshing itself back into eligibility and get re-rendered at real cost.
    _age_cutoff = job_age_cutoff()
    if worker_id:
        # Either: unclaimed, OR claimed by this same worker
        _q = db.query(Clip).join(Job).filter(
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
        )
        if _age_cutoff is not None:
            _q = _q.filter(Job.created_at >= _age_cutoff)
        redo_clips = _q.order_by(Clip.id.asc()).all()
    else:
        # No worker_id - get unclaimed only (legacy behavior)
        _q = db.query(Clip).join(Job).filter(
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
        )
        if _age_cutoff is not None:
            _q = _q.filter(Job.created_at >= _age_cutoff)
        redo_clips = _q.order_by(Clip.id.asc()).all()

    if _age_cutoff is not None:
        _skipped = db.query(Clip).join(Job).filter(
            Job.backend == 'flow',
            Clip.status == ClipStatus.FLOW_REDO_QUEUED.value,
            Job.created_at < _age_cutoff,
        ).count()
        if _skipped:
            print(f"[redo-pending] age cap: skipped {_skipped} clip(s) "
                  f"on jobs older than {_age_cutoff.isoformat()}", flush=True)

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
            "prompt_b": clip.prompt_text_b,  # v805 — policy-fallback prompt (voice-only)
            "dialogue_text_b": clip.dialogue_text_b,  # v821 — reworded Prompt B line
            "rendered_prompt_variant": clip.rendered_prompt_variant,  # v821 — A/B variant
            "language": job_config.get("language", "English"),
            "duration": job_config.get("duration", "8"),
            "voice_profile": job_config.get("voice_profile", "") or job_config.get("user_context", ""),
            "start_frame_url": f"{base_url}/api/local-worker/frames/{job.id}/{start_filename}" if start_filename else None,
            "end_frame_url": f"{base_url}/api/local-worker/frames/{job.id}/{end_filename}" if end_filename else None,
            "flow_project_url": job.flow_project_url,
            "generation_attempt": clip.generation_attempt,
            "redo_reason": clip.redo_reason,
            "error_message": clip.error_message,  # v849 — carries the durable Prompt-B requeue marker so a restarted worker re-derives the reworded-line intent
            "claimed_by": clip.claimed_by_worker,
            # Storyboard/Scene mode fields for continue mode support
            "clip_mode": clip.clip_mode or "fresh",
            "scene_index": clip.scene_index or 0,
            # v861 — per-clip render duration (4|6|8|10). NULL → the worker
            # falls back to the job-level duration (legacy / manual jobs).
            "veo_render_duration_s": clip.veo_render_duration_s,
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


def _clip_id_as_int(clip_id):
    """Clip.id is an Integer column. A non-numeric path value can never match a
    row, but if it reaches the SQL layer Postgres raises DataError ("invalid
    input syntax for type integer") — a 500 with a full traceback — instead of
    the 404 the semantics imply. Production has been receiving clip_id "d" on
    the worker status route every few hours since at least 2026-08-21 (caller
    unknown; the guard's log line fingerprints it). Returns int or None."""
    try:
        s = str(clip_id).strip()
    except Exception:
        return None
    return int(s) if s.isdigit() else None


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
    # v821 — worker reports which prompt variant produced the render
    # ("A" = original, "B" = reworded voice-only fallback). Optional so
    # older workers / non-completion updates leave the stored value alone.
    rendered_prompt_variant: Optional[str] = None


@app.post("/api/local-worker/clips/{clip_id}/status")
async def local_worker_update_clip_status(
    clip_id: str,
    update: LocalWorkerClipUpdate,
    request: Request,
    db: DBSession = Depends(get_db_session),
    authorized: bool = Depends(verify_local_worker_key)
):
    """Update clip status"""
    cid = _clip_id_as_int(clip_id)
    if cid is None:
        _client = request.client.host if request.client else "?"
        print(
            f"[clip-status-guard] non-numeric clip_id {clip_id!r} on local-worker route "
            f"from {_client} ua={request.headers.get('user-agent', '?')!r} "
            f"status={update.status!r} err={(update.error_message or '')[:120]!r}",
            flush=True,
        )
        raise HTTPException(status_code=404, detail="Clip not found")
    # Use FOR UPDATE to prevent race condition with upload endpoint
    clip = db.query(Clip).filter(Clip.id == cid).with_for_update().first()
    if not clip:
        raise HTTPException(status_code=404, detail="Clip not found")
    
    # Clean up images_dir for Flow jobs (frames are in R2, not local disk)
    # This fixes existing Flow jobs that still have local paths set
    job = db.query(Job).filter(Job.id == clip.job_id).first()
    # v921 — work heartbeat, same as the user-worker endpoint. See the comment
    # there: Job.updated_at is the only per-job liveness signal we have, and
    # without this it stays frozen at the moment the job went 'processing'.
    if job:
        job.updated_at = datetime.utcnow()
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
    # v821 — persist the prompt variant that produced this render. Only when
    # the worker sends a value (don't clobber the stored A/B default with None).
    if update.rendered_prompt_variant is not None:
        clip.rendered_prompt_variant = update.rendered_prompt_variant

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
            # Check if all clips are completed (v825 guard: skip on total 0/None)
            if job.total_clips and completed >= job.total_clips:
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


# =============================================================================
# v943 — character-swap source videos
# =============================================================================
# The build names an asset; the platform stores an opaque R2 key. These three
# endpoints are the whole transport: one to put the mp4 in R2, two for the
# worker to fetch what a swap clip needs (the source video and the one avatar
# image it swaps in). Everything is prefix-locked to swap-sources/ so a worker
# key can never be used to read arbitrary storage.

SWAP_SOURCE_PREFIX = "swap-sources/"

# v943 — what the upload route will accept. The route is authenticated, but
# "authenticated" is not "bounded": before these existed the handler copied an
# UploadFile until EOF and pushed whatever came out to R2, so one wrong path on
# a command line could have parked a feature film in the bucket.
#
# 80MB is the byte cap. A 10s 1080p30 h264 clip off a phone or a reel ripper is
# 3-25MB; 80MB leaves room for a high-bitrate or ProRes-ish export of the same
# length while still refusing anything of a different order.
SWAP_SOURCE_MAX_BYTES = 80 * 1024 * 1024
# The worker trims every source to this before rendering (CHARSWAP_MAX_SOURCE_S
# in flow_worker.py). It is the render cap, not the upload cap.
SWAP_SOURCE_RENDER_CAP_S = 10
# The upload cap carries 2s of slack on top of it on purpose: a 10.2s cut is
# a correct source that the worker trims, while a 30s reel is someone sending
# the wrong file. Refuse the second, accept the first.
SWAP_SOURCE_MAX_DURATION_S = 12.0


def _v943_probe_source(path):
    """ffprobe a spooled upload. Separate function so tests can replace it."""
    from video_processor import ffprobe_json
    return ffprobe_json(Path(path))


def _v943_validate_swap_source(tmp_path, name=""):
    """Refuse anything that is not a short mp4. Returns the duration in seconds.

    Checked on the spooled temp file, before a single byte reaches R2. The
    caller deletes the temp file either way — this only decides the verdict.
    """
    try:
        info = _v943_probe_source(tmp_path)
    except Exception as e:
        raise HTTPException(
            status_code=415,
            detail=f"Swap source {name!r} is not readable video ({e})")

    fmt = ((info.get("format") or {}).get("format_name") or "").lower()
    streams = info.get("streams") or []
    has_video = any(s.get("codec_type") == "video" for s in streams)
    # ffprobe reports the mp4 family as 'mov,mp4,m4a,3gp,3g2,mj2'.
    if "mp4" not in fmt or not has_video:
        raise HTTPException(
            status_code=415,
            detail=f"Swap source {name!r} must be an mp4 with a video stream "
                   f"(ffprobe says format={fmt or 'unknown'}, "
                   f"video_stream={has_video})")

    try:
        duration = float((info.get("format") or {}).get("duration") or 0)
    except (TypeError, ValueError):
        duration = 0.0
    if duration <= 0:
        raise HTTPException(
            status_code=415,
            detail=f"Swap source {name!r} reports no duration")
    if duration > SWAP_SOURCE_MAX_DURATION_S:
        raise HTTPException(
            status_code=422,
            detail=f"Swap source {name!r} is {duration:.1f}s; the charswap "
                   f"route takes clips up to {SWAP_SOURCE_MAX_DURATION_S:.0f}s "
                   f"(the worker renders the first "
                   f"{SWAP_SOURCE_RENDER_CAP_S}s). Cut it first.")
    return duration


@app.post("/api/images/swap-sources")
async def upload_swap_source_video(
    file: UploadFile = File(...),
    name: str = Form(...),
    current_user: User = Depends(get_current_user),
):
    """Store one source mp4 for a charswap scene and hand back its R2 key.

    Streamed to a temp file and then to R2 — never held in memory. A source
    clip is small but these land while renders and exports are running, which
    is exactly when a 10-40MB transient allocation hurts (same reason as v872
    on the clip-upload path).

    v943 — bounded on the way in: the copy stops at SWAP_SOURCE_MAX_BYTES, and
    the spooled file must ffprobe as a short mp4 before it is stored. A
    rejected upload leaves nothing behind on disk or in R2.
    """
    from backends.storage import is_storage_configured, get_storage
    import tempfile
    import uuid as _uuid
    import re  # main.py has no module-level bare `re`

    if not is_storage_configured():
        raise HTTPException(status_code=503, detail="Storage not configured")

    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "-", (name or "source")).strip("-") or "source"
    r2_key = f"{SWAP_SOURCE_PREFIX}{current_user.id}/{_uuid.uuid4().hex}_{safe_name}.mp4"

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
            tmp_path = tmp.name
        try:
            size = await asyncio.to_thread(
                _spool_upload_to_path, file, tmp_path,
                1 << 20, SWAP_SOURCE_MAX_BYTES)
        except UploadTooLarge:
            raise HTTPException(
                status_code=413,
                detail=f"Swap source {name!r} is larger than "
                       f"{SWAP_SOURCE_MAX_BYTES // (1 << 20)} MB")
        duration = await asyncio.to_thread(
            _v943_validate_swap_source, tmp_path, name)
        storage = get_storage()
        await asyncio.to_thread(storage.upload_file, tmp_path, r2_key, "video/mp4")
        # TEMP DIAG [TEMP] (remove once one charswap render is confirmed live)
        print(f"[v943] swap source stored: name={name!r} bytes={size} "
              f"duration={duration:.1f}s key={r2_key}", flush=True)
        return {"success": True, "name": name, "r2_key": r2_key,
                "bytes": size, "duration_s": round(duration, 2)}
    except HTTPException as e:
        # A refusal is the route working. Say why, with the status it earned,
        # instead of laundering it into a 500.
        print(f"[v943] swap source refused for {name!r}: "
              f"{e.status_code} {e.detail}", flush=True)
        raise
    except Exception as e:
        print(f"[v943] swap source upload failed for {name!r}: {e}", flush=True)
        raise HTTPException(status_code=500, detail=f"Swap source upload failed: {e}")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def _v943_swap_source_key_owner(key: str):
    """The user id a swap-source key belongs to, or None if it is not one.

    Ownership is IN the key: the upload route writes every source to
    `swap-sources/{user_id}/…` and the import refuses a key outside the
    importing user's prefix, so the segment after the prefix is the owner.
    """
    if not key or not key.startswith(SWAP_SOURCE_PREFIX) or ".." in key:
        return None
    rest = key[len(SWAP_SOURCE_PREFIX):]
    owner, sep, remainder = rest.partition("/")
    if not owner or not sep or not remainder:
        return None
    return owner


def _v943_swap_source_owned_by(key: str, user_id) -> bool:
    """True when `key` is a swap-source key belonging to `user_id`."""
    return _v943_swap_source_key_owner(key) == str(user_id)


async def _v943_swap_source_response(key: str, user_id=None):
    """Stream a charswap source video out of R2.

    The key arrives from the clip payload, so it is checked against the
    swap-sources/ prefix before anything is read: a worker credential must not
    turn into a way to read any object in the bucket.

    v943 owner scoping — a user-worker token authenticates ONE user, and this
    is where that fact is spent. Pass `user_id` and the key must sit under that
    user's prefix, so a token cannot read another account's source clip. The
    shared local-worker key belongs to no user and passes None; it is the
    operator's own admin credential, not a per-account one.

    The temp file is deleted when the response finishes, not only when the
    download fails — see the BackgroundTask below.
    """
    from fastapi.responses import FileResponse
    from starlette.background import BackgroundTask
    from backends.storage import is_storage_configured, get_storage
    import tempfile

    if not key.startswith(SWAP_SOURCE_PREFIX) or ".." in key:
        raise HTTPException(status_code=400, detail="Not a swap-source key")
    if user_id is not None and not _v943_swap_source_owned_by(key, user_id):
        print(f"[v943] swap source DENIED: key={key} not owned by {user_id}",
              flush=True)
        raise HTTPException(status_code=404, detail="Swap source not found")
    if not is_storage_configured():
        raise HTTPException(status_code=503, detail="Storage not configured")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
        tmp_path = tmp.name
    try:
        await asyncio.to_thread(get_storage().download_file, key, tmp_path)
    except Exception as e:
        _v943_unlink(tmp_path)
        print(f"[v943] swap source download failed key={key}: {e}", flush=True)
        raise HTTPException(status_code=404, detail="Swap source not found")
    # FileResponse streams from disk and then runs `background`. Deleting the
    # file here instead of at the next failure is the difference between a
    # night of worker retries costing nothing and it filling Render's temp
    # disk — every successful download used to leave its copy behind.
    return FileResponse(tmp_path, media_type="video/mp4",
                        filename=key.rsplit("/", 1)[-1],
                        background=BackgroundTask(_v943_unlink, tmp_path))


def _v943_unlink(path):
    """Delete a served temp file. Never raises — it runs after the response."""
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[v943] temp cleanup failed for {path}: {e}", flush=True)


async def _v943_swap_avatar_response(node_id: int, user_id=None):
    """Stream the chosen image of the ONE upload a swap clip swaps in.

    Bound at import (image_platform resolves exactly one character upload and
    refuses the import otherwise), so this only ever serves a node the build
    already named.

    v943 owner scoping — `user_id` narrows the query to that user's nodes, so
    a worker token cannot walk the id space and pull faces out of other
    accounts. The node must also be `kind='upload'`: a swap swaps in a real
    uploaded face, and import already refuses anything else, so a generated
    node arriving here means the id came from somewhere it should not have.
    """
    from fastapi.responses import Response as _Resp
    from image_platform import (
        ImageNode as _Node, ImageVariant as _Variant,
        images_root as _images_root, _storage_download_to_local as _pull,
    )
    from models import get_db as _get_db

    with _get_db() as db:
        q = db.query(_Node).filter(_Node.id == node_id, _Node.kind == "upload")
        if user_id is not None:
            q = q.filter(_Node.user_id == str(user_id))
        node = q.first()
        if node is None or node.chosen_variant_id is None:
            if user_id is not None:
                print(f"[v943] swap avatar DENIED or missing: node={node_id} "
                      f"user={user_id}", flush=True)
            raise HTTPException(status_code=404, detail=f"Upload {node_id} has no chosen image")
        variant = db.query(_Variant).filter(_Variant.id == node.chosen_variant_id).first()
        if variant is None:
            raise HTTPException(status_code=404, detail=f"Upload {node_id} variant missing")
        rel_path = variant.image_path

    local_path = _images_root() / rel_path
    if not local_path.exists():
        await asyncio.to_thread(_pull, rel_path)
    if not local_path.exists():
        raise HTTPException(status_code=404, detail=f"Upload {node_id} image unavailable")
    data = await asyncio.to_thread(local_path.read_bytes)
    media_type = "image/png" if str(rel_path).lower().endswith(".png") else "image/jpeg"
    return _Resp(content=data, media_type=media_type)


def _v943_maybe_charswap(clip_data: dict, clip, base_url: str, lane: str) -> dict:
    """Add the charswap keys to a clip payload, but only for a swap clip.

    A legacy clip comes back with the dict it went in with — same keys, same
    values. That is the whole regression contract for the no-metadata path:
    a Veo render is stochastic and can never be byte-compared, but the JSON
    the worker is handed can.
    """
    if (getattr(clip, "render_method", None) or "") != "charswap":
        return clip_data
    clip_data.update(_v943_charswap_payload(clip, base_url, lane))
    return clip_data


def _v943_charswap_payload(clip, base_url: str, lane: str) -> dict:
    """The extra job-payload keys a charswap clip needs, and nothing else.

    `lane` picks which authenticated download path the worker should call —
    the two worker lanes carry different credentials, so they get their own
    URLs even though the bytes behind them are identical.
    """
    from urllib.parse import quote

    avatar_id = clip.swap_avatar_upload_id
    src_key = clip.swap_source_r2_key
    return {
        "render_method": clip.render_method,
        "swap_mode": clip.swap_mode or "video-led",
        "swap_source_key": src_key,
        "swap_source_url": (
            f"{base_url}/api/{lane}/swap-source?key={quote(src_key, safe='')}"
            if src_key else None
        ),
        "swap_avatar_upload_id": avatar_id,
        "swap_avatar_url": (
            f"{base_url}/api/{lane}/swap-avatar/{avatar_id}"
            if avatar_id else None
        ),
        # v943 — the cap the swap route was measured against. The worker trims
        # to it rather than trusting the file, so a longer source degrades to a
        # short render instead of a refused one.
        "swap_max_source_s": 10,
    }


@app.get("/api/local-worker/swap-source")
async def local_worker_download_swap_source(
    key: str = Query(..., description="R2 key from the clip's swap_source_r2_key"),
    authorized: bool = Depends(verify_local_worker_key),
):
    """Charswap source video, for a worker holding the local-worker key."""
    return await _v943_swap_source_response(key)


@app.get("/api/local-worker/swap-avatar/{node_id}")
async def local_worker_download_swap_avatar(
    node_id: int,
    authorized: bool = Depends(verify_local_worker_key),
):
    """Charswap avatar image, for a worker holding the local-worker key."""
    return await _v943_swap_avatar_response(node_id)


# The user-worker twins of these two live further down the file, next to
# verify_user_worker_token — the dependency has to exist before they are
# declared.


@app.get("/api/admin/verify-charswap-columns")
async def verify_charswap_columns_endpoint(
    current_user: User = Depends(get_current_user),
):
    """Prove the v943 columns exist and can be read on the live database.

    Startup catches a failed image migration and keeps serving, so a healthy
    deploy is not evidence the columns landed. This is the evidence.
    """
    from image_platform import verify_charswap_columns as _verify
    result = await asyncio.to_thread(_verify)
    print(f"[v943] column readback: {result}", flush=True)
    return result


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
        
        # Save uploaded file temporarily.
        # v872 — streamed in 1MB chunks, not read() into one bytes object. These
        # POSTs land WHILE an export is running (be09f595 log), i.e. at the
        # worst possible moment for a 10-40MB transient allocation.
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as tmp:
            tmp_path = tmp.name
        await asyncio.to_thread(_spool_upload_to_path, file, tmp_path)

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


# ============ Auto-edit worker endpoints ============
# The user-facing half (queue / status / templates) sits up by export-status.
# These four live here because Depends(verify_user_worker_token) is resolved
# while the module executes, so the name must already be defined above.
# Every one scopes on user_id: a worker only ever sees its own account's rows.


def _autoedit_run_for_worker(db, autoedit_id: str, worker_user_id: str):
    """Look up a run scoped to the worker's own account. 404 on someone else's
    row — that leaks nothing, and it is also the honest answer for the worker."""
    from models import AutoEditRun
    run = db.query(AutoEditRun).filter(
        AutoEditRun.id == autoedit_id,
        AutoEditRun.user_id == worker_user_id,
    ).first()
    if not run:
        raise HTTPException(status_code=404, detail="unknown run")
    return run


@app.post("/api/autoedit/claim")
async def claim_autoedit(
    db: DBSession = Depends(get_db_session),
    worker_user_id: str = Depends(verify_user_worker_token),
):
    """Hand the worker its next runnable row, or `autoedit_id: None`."""
    from models import AutoEditRun
    from autoedit_queue import is_claimable
    from datetime import datetime as _dt

    now = _dt.utcnow()
    # skip_locked: without the row lock, two workers polling at the same instant
    # both read the same row, both pass is_claimable, both commit — two renders
    # of one job. Postgres (production) honours FOR UPDATE SKIP LOCKED; SQLite
    # (local tests) silently renders no lock at all, which is why the lock looks
    # like it does nothing when you run the test suite.
    for run in db.query(AutoEditRun).filter(
            AutoEditRun.user_id == worker_user_id,
            AutoEditRun.state.in_(["queued", "claimed", "running"])
    ).order_by(AutoEditRun.created_at.asc()).with_for_update(skip_locked=True).all():
        if is_claimable(run.state, run.heartbeat_at, now):
            run.state, run.claimed_by, run.heartbeat_at = "claimed", worker_user_id[:8], now
            # A stale reclaim counts too, so the MAX_ATTEMPTS cap means "attempts
            # including reclaims" — a worker that keeps crashing burns the budget.
            run.attempts += 1
            db.commit()
            print(f"[AutoEdit] claimed {run.id} by {run.claimed_by} attempt={run.attempts}",
                  flush=True)
            return run.to_dict()
    return {"autoedit_id": None}


class AutoEditProgress(BaseModel):
    stage: str


@app.post("/api/autoedit/{autoedit_id}/progress")
async def autoedit_progress(
    autoedit_id: str,
    p: AutoEditProgress,
    db: DBSession = Depends(get_db_session),
    worker_user_id: str = Depends(verify_user_worker_token),
):
    """Heartbeat + stage label. Silence for STALE_AFTER makes the row claimable again."""
    from datetime import datetime as _dt

    run = _autoedit_run_for_worker(db, autoedit_id, worker_user_id)
    run.state, run.stage, run.heartbeat_at = "running", p.stage, _dt.utcnow()
    db.commit()
    return {"ok": True}


@app.post("/api/autoedit/{autoedit_id}/complete")
async def autoedit_complete(
    autoedit_id: str,
    video: UploadFile = File(...),
    qc_report: str = Form("{}"),
    db: DBSession = Depends(get_db_session),
    worker_user_id: str = Depends(verify_user_worker_token),
):
    """Worker uploads the finished mp4; we store it next to the job's outputs.

    v938 — the storing itself lives in _autoedit_store_result, shared with the
    server-side executor, so the two finishers cannot drift apart. Only the
    reading of the bytes is different here: an upload stream, with a size cap.
    """
    run = _autoedit_run_for_worker(db, autoedit_id, worker_user_id)

    try:
        parsed = json.loads(qc_report or "{}")
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="qc_report must be valid JSON")
    qc = _autoedit_normalize_qc(parsed, "The local worker")

    async def _write(tmp: Path):
        written = 0
        with open(tmp, "wb") as f:
            while chunk := await video.read(1 << 20):
                written += len(chunk)
                if written > AUTOEDIT_MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=f"Auto-edit upload is larger than "
                               f"{AUTOEDIT_MAX_UPLOAD_BYTES // (1 << 20)} MB")
                f.write(chunk)

    return await _autoedit_store_result(db, run, qc, _write)


class AutoEditFail(BaseModel):
    error: str


@app.post("/api/autoedit/{autoedit_id}/fail")
async def autoedit_fail(
    autoedit_id: str,
    p: AutoEditFail,
    db: DBSession = Depends(get_db_session),
    worker_user_id: str = Depends(verify_user_worker_token),
):
    """Worker reports a failure. Back to `queued` until MAX_ATTEMPTS is spent.

    v938 — the retry decision lives in _autoedit_apply_failure, shared with the
    server-side executor so both paths spend the attempt budget the same way.
    """
    run = _autoedit_run_for_worker(db, autoedit_id, worker_user_id)
    _autoedit_apply_failure(run, p.error)
    db.commit()
    print(f"[AutoEdit] fail {run.id} attempt={run.attempts} -> {run.state}: {p.error[:120]}",
          flush=True)
    return {"ok": True, "state": run.state}


@app.get("/api/user-worker/swap-source")
async def user_worker_download_swap_source(
    key: str = Query(..., description="R2 key from the clip's swap_source_r2_key"),
    user_id: str = Depends(verify_user_worker_token),
):
    """v943 — charswap source video, for a worker holding a user worker token.

    Declared here because the token dependency is defined further up in this
    section. Unlike the local-worker twin, the token names a user, so the key
    is checked against THAT user's prefix — the token authenticates one
    account and must only read that account's sources.
    """
    return await _v943_swap_source_response(key, user_id=user_id)


@app.get("/api/user-worker/swap-avatar/{node_id}")
async def user_worker_download_swap_avatar(
    node_id: int,
    user_id: str = Depends(verify_user_worker_token),
):
    """v943 — charswap avatar image, for a worker holding a user worker token.

    Scoped to the token's user: an ImageNode id is a small integer, so an
    unscoped lookup here would serve any account's uploaded face to anyone
    holding any worker token.
    """
    return await _v943_swap_avatar_response(node_id, user_id=user_id)


# v899 — per-worker liveness for the Flow worker: user_id -> (worker_id, ts).
# UserWorkerToken.last_seen is refreshed by ANY authenticated call on that token,
# and the operator runs image_worker.py and chatgpt_image_worker.py on the SAME
# token, so it cannot answer "is the Flow worker up?". Only /heartbeat carries a
# worker_id and only flow_worker posts there, so this map is Flow-specific.
_FLOW_WORKER_BEATS = {}


def flow_worker_online(user_id, now=None, window_s=15):
    """True if a FLOW worker heartbeat arrived inside the window.

    Returns None when this user has never sent a worker_id heartbeat, so the
    caller can fall back to the old token.last_seen behaviour for workers too
    old to send one. Never let 'unknown' read as 'offline' — that would show a
    working worker as down.
    """
    beat = _FLOW_WORKER_BEATS.get(user_id)
    if not beat:
        return None
    _wid, ts = beat
    return ((now or datetime.utcnow()) - ts).total_seconds() < window_s


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
    worker_id = None
    try:
        body = await request.json()
        going_offline = bool(body.get("going_offline"))
        worker_id = (body.get("worker_id") or "").strip() or None
    except Exception:
        going_offline = False

    now = datetime.utcnow()
    if going_offline:
        # Backdate beyond the 15s window — UI sees Offline on next poll.
        token.last_seen = now - timedelta(seconds=3600)
        _FLOW_WORKER_BEATS.pop(token.user_id, None)
        # v780 diagnostic — clean-stop signal landed. Low frequency (once per
        # worker shutdown), so it doesn't spam. Remove once evidence confirms.
        print(f"[v780] user-worker going_offline user={token.user_id}", flush=True)
    else:
        token.last_seen = now
        # v899 — record liveness for the FLOW worker specifically, not just the
        # token. last_seen lives on UserWorkerToken and the operator runs several
        # workers (flow, image_worker, chatgpt_image_worker) on the SAME token, so
        # any of them refreshing it made the My Worker dot claim the Flow worker
        # was up when it had been stopped. Only this endpoint carries worker_id,
        # and only flow_worker calls it, so this key is Flow-specific by
        # construction. In-memory on purpose (same pattern as _worker_errors): a
        # Render restart just means the dot waits for the next 5s heartbeat.
        if worker_id:
            _FLOW_WORKER_BEATS[token.user_id] = (worker_id, now)
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

    # v921 — recover jobs STRANDED at 'processing' by a worker that died.
    #
    # The sweep above only covers 'pending' / 'queued_for_flow'. A worker that
    # dies mid-job (crash, OOM, Render deploy, kill) leaves its job at
    # 'processing' with the claim still set, and NOTHING released it — the job
    # sat there forever while the worker polled "No pending jobs or redos".
    # Measured 2026-08-07 on job 09083c15: stranded until reset by hand.
    #
    # The cutoff is deliberately generous and keyed on updated_at, NOT
    # claimed_at. A legitimate 15-clip job runs far longer than the 10-minute
    # claim window, so releasing on claim age would hand a live worker's job to
    # a second worker and double-submit its clips. updated_at is now touched on
    # every clip report (v921 work heartbeat in the clips/{id}/status
    # endpoints), so 30 minutes of total silence means the worker is gone.
    stranded_cutoff = datetime.utcnow() - timedelta(minutes=30)
    stranded_jobs = db.query(Job).filter(
        Job.user_id == user_id,
        Job.backend == 'flow',
        Job.status == 'processing',
        Job.updated_at < stranded_cutoff
    ).all()

    for sj in stranded_jobs:
        print(f"[Worker] v921 releasing STRANDED job {sj.id[:8]} "
              f"(processing, no clip activity since {sj.updated_at}, "
              f"was claimed by {sj.claimed_by_worker})", flush=True)
        sj.claimed_by_worker = None
        sj.claimed_at = None
        sj.status = 'pending'
    if stranded_jobs:
        db.commit()

    # Query for available jobs - SCOPED TO USER
    _age_cutoff = job_age_cutoff()
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

        if _age_cutoff is not None:
            query = query.filter(Job.created_at >= _age_cutoff)

        job = query.order_by(Job.created_at.asc()).first()

        if job:
            job.claimed_by_worker = worker_id
            job.claimed_at = datetime.utcnow()
            if job.status == 'pending':
                job.status = 'queued_for_flow'
            db.commit()
    else:
        # No worker_id -> LOOK WITHOUT CLAIMING. exclude must still apply here:
        # it used to be honoured only in the worker_id branch above, so a caller
        # paging the queue read-only got handed the same head job forever (a
        # 40-step walk returned one job 40 times). Without this, the only way to
        # see past the head is to claim, which mutates live queue state just to
        # browse it.
        query = db.query(Job).filter(
            Job.user_id == user_id,
            Job.backend == 'flow',
            Job.status.in_(['pending', 'queued_for_flow']),
            Job.claimed_by_worker.is_(None)
        )
        if exclude_ids:
            query = query.filter(Job.id.notin_(exclude_ids))
            # TEMP DIAG (remove once operator-side evidence lands): proves the
            # read-only branch now honours exclude. Before this fix a caller
            # paging without worker_id got the same head job forever.
            print(f"[UserWorker] read-only pending poll excluding {len(exclude_ids)} job(s)",
                  flush=True)

        if _age_cutoff is not None:
            query = query.filter(Job.created_at >= _age_cutoff)

        job = query.order_by(Job.created_at.asc()).first()

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
        
        _clip_data = {
            "id": clip.id,
            "clip_index": clip.clip_index,
            "dialogue_text": clip.dialogue_text,
            "prompt": clip.prompt_text,
            "prompt_b": clip.prompt_text_b,  # v805 — policy-fallback prompt (voice-only)
            "start_frame_key": start_frame_key,
            "end_frame_key": end_frame_key,
            "status": clip.status,
            "start_frame_url": f"{base_url}/api/user-worker/frames/{job.id}/{start_filename}" if start_filename else None,
            "end_frame_url": f"{base_url}/api/user-worker/frames/{job.id}/{end_filename}" if end_filename else None,
            "clip_mode": clip.clip_mode or "fresh",
            "scene_index": clip.scene_index or 0,
            # v861 — per-clip render duration (4|6|8|10). NULL → the worker
            # falls back to the job-level duration (legacy / manual jobs).
            "veo_render_duration_s": clip.veo_render_duration_s,
        }
        # v943 — see the local-worker payload; keys appear only on a swap clip.
        _clip_data = _v943_maybe_charswap(_clip_data, clip, base_url, "user-worker")
        clips_data.append(_clip_data)
    
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
        # Job.created_at is immutable, unlike updated_at above which the redo path
        # and the startup backfill both bump — that is why an ancient job could keep
        # refreshing itself back into eligibility and get re-rendered at real cost.
        _age_cutoff = job_age_cutoff()
        _q = db.query(Clip).join(Job).filter(
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
        )
        if _age_cutoff is not None:
            # v932.1 — a recreated clip is a deliberate operator act on a
            # possibly old job; its redo_reason marks it and the 24h
            # Job.updated_at filter above still bounds it. The age cap keeps
            # excluding STALE queue entries (the 2026-08-07 failure), which
            # never carry this marker. v933 modify twins get the same pass.
            _q = _q.filter(or_(Job.created_at >= _age_cutoff,
                               Clip.redo_reason.like('v932 recreate%'),
                               Clip.redo_reason.like('v933 modify%')))
        redo_clips = _q.order_by(Clip.id.asc()).all()
    else:
        redo_cutoff = datetime.utcnow() - timedelta(hours=24)
        # Job.created_at is immutable, unlike updated_at above which the redo path
        # and the startup backfill both bump — that is why an ancient job could keep
        # refreshing itself back into eligibility and get re-rendered at real cost.
        _age_cutoff = job_age_cutoff()
        _q = db.query(Clip).join(Job).filter(
            Job.user_id == user_id,
            Job.backend == 'flow',
            Job.updated_at >= redo_cutoff,
            or_(
                and_(Clip.status == ClipStatus.FLOW_REDO_QUEUED.value, Clip.claimed_by_worker.is_(None)),
                and_(Clip.status == 'failed', Clip.generation_attempt > 1, Clip.error_message.ilike('%file not found%'))
            )
        )
        if _age_cutoff is not None:
            # v932.1/v933 — same deliberate-recreate exemption as the worker_id branch.
            _q = _q.filter(or_(Job.created_at >= _age_cutoff,
                               Clip.redo_reason.like('v932 recreate%'),
                               Clip.redo_reason.like('v933 modify%')))
        redo_clips = _q.order_by(Clip.id.asc()).all()

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
            "prompt_b": clip.prompt_text_b,  # v805 — policy-fallback prompt (voice-only)
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
            "error_message": clip.error_message,  # v849 — carries the durable Prompt-B requeue marker so a restarted worker re-derives the reworded-line intent
            "claimed_by": clip.claimed_by_worker,
            "clip_mode": clip.clip_mode or "fresh",
            "scene_index": clip.scene_index or 0,
            # v861 — per-clip render duration (4|6|8|10). NULL → the worker
            # falls back to the job-level duration (legacy / manual jobs).
            "veo_render_duration_s": clip.veo_render_duration_s,
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

        # Prompt for Kling i2v: use the clip's BUILT Veo prompt (clip.prompt_text —
        # action/camera + any veo_prompt_override folded in by build_prompt), NOT the
        # bare spoken line. Fall back to the override from dialogue_json, then the
        # dialogue line, then a generic motion prompt.
        prompt = (clip.prompt_text or "").strip()
        if not prompt:
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
        if not prompt:
            prompt = (clip.dialogue_text or "").strip()
        if not prompt:
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

    # v812 — audio-twin anchor auto-swap (see local-worker copy for rationale).
    _v812_key = _v812_audio_anchor_fallback(db, clip, rejected_key)
    if _v812_key:
        return _v812_apply_swap(db, clip, rejected_key, _v812_key)

    # v815 — prominent-people / celebrity auto-retry branch. Scoped to
    # this reason ONLY; generic content-policy keeps the manual card path.
    is_prominent = (request.error_reason or "").upper()
    is_prominent = ("PROMINENT" in is_prominent) or ("CELEBRITY" in is_prominent)
    if is_prominent:
        clip.error_code = "PROMINENT_PEOPLE_FILTER"
        clip.error_message = request.detail or "Rejected (prominent people). Auto-retry in progress."
        if rejected_key:
            clip.replacement_start_frame = rejected_key
        # v815 — NO commit here: let the outcome commit atomically so a
        # retry failure can't leave the clip stamped-but-not-swapped.
        applied = None
        try:
            applied = _auto_image_retry(db, clip, rejected_key)
        except Exception as _ar_err:
            import traceback
            print(f"[v815] auto-image-retry FAILED for clip {clip_id}: "
                  f"{type(_ar_err).__name__}: {_ar_err}", flush=True)
            traceback.print_exc()
            db.rollback()
            applied = None
        if applied:
            return {"ok": True, "clip_id": clip_id, "auto_retry": applied}
        # disabled / exhausted / errored -> manual card, single atomic
        # commit. Re-stamp: db.rollback() discarded the in-memory fields.
        clip.error_code = "PROMINENT_PEOPLE_FILTER"
        clip.error_message = request.detail or "Rejected (prominent people). Upload a replacement to retry."
        if rejected_key:
            clip.replacement_start_frame = rejected_key
        clip.status = ClipStatus.FAILED.value
        db.commit()
        return {"ok": True, "clip_id": clip_id, "auto_retry": None,
                "rejected_image_key": rejected_key or None}
    # (existing generic CONTENT_POLICY_VIOLATION path continues unchanged below)

    if rejected_key:
        clip.replacement_start_frame = rejected_key

    clip.status = ClipStatus.FAILED.value
    clip.error_code = "CONTENT_POLICY_VIOLATION"
    # v899.5 — KEEP THE SPECIFIC REASON. Only PROMINENT/CELEBRITY got a precise
    # code above; every other rejection collapsed to the generic string and the
    # worker's error_reason (PUBLIC_ERROR_SEXUAL, PUBLIC_ERROR_UNSAFE_GENERATION,
    # ...) was thrown away. That is why "why was this clip redone?" had no
    # answer on job f58e833f (2026-08-18) — the only copy lived in a console.
    # Flow's own code is the actionable part: SEXUAL means reshoot the frame,
    # UNSAFE_GENERATION often clears on a reworded line.
    _flow_reason = (request.error_reason or "").strip()
    clip.error_message = (
        request.detail
        or "⚠️ Flow rejected this image's content. Upload a replacement to retry."
    )
    if _flow_reason:
        clip.error_message = f"{clip.error_message} [Flow: {_flow_reason}]"
        _rr = f"content policy: {_flow_reason}"
        clip.redo_reason = (
            f"{clip.redo_reason} | {_rr}" if clip.redo_reason else _rr
        )[:1000]
    print(f"[v899.5] clip {clip_id} content policy | flow_reason="
          f"{_flow_reason or 'NOT SENT BY WORKER'}", flush=True)
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
                # v812 — never pre-fail an audio twin: it self-heals via
                # its OWN violation report (anchor auto-swap + requeue).
                if (sib.clip_role or '') == 'audio_pair':
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
    request: Request,
    db: DBSession = Depends(get_db_session),
    user_id: str = Depends(verify_user_worker_token)
):
    """Update clip status - verified ownership."""
    cid = _clip_id_as_int(clip_id)
    if cid is None:
        _client = request.client.host if request.client else "?"
        print(
            f"[clip-status-guard] non-numeric clip_id {clip_id!r} on user-worker route "
            f"from {_client} ua={request.headers.get('user-agent', '?')!r} "
            f"status={update.status!r} err={(update.error_message or '')[:120]!r}",
            flush=True,
        )
        raise HTTPException(status_code=404, detail="Clip not found or not yours")
    clip = db.query(Clip).join(Job).filter(Clip.id == cid, Job.user_id == user_id).with_for_update().first()
    if not clip:
        raise HTTPException(status_code=404, detail="Clip not found or not yours")

    job = db.query(Job).filter(Job.id == clip.job_id).first()
    if job and job.backend == 'flow' and job.images_dir:
        job.images_dir = ""

    # v921 — work heartbeat. Clip has no updated_at column (only Job does,
    # models.py:167), and the common path here writes ONLY clip fields, so a job
    # could run for an hour with Job.updated_at frozen at the moment it went
    # 'processing'. That left no way to tell a live job from one whose worker
    # died. Touching updated_at on every clip report makes it a true
    # "work is still happening" signal, which the stranded-job sweep in
    # /jobs/pending relies on to avoid stealing a job from a live worker.
    if job:
        job.updated_at = datetime.utcnow()

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
            # v825 guard: skip status flip on total 0/None
            if job.total_clips and completed >= job.total_clips:
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
        
        # v872 — streamed to disk in 1MB chunks (see _spool_upload_to_path).
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as tmp:
            tmp_path = tmp.name
        await asyncio.to_thread(_spool_upload_to_path, file, tmp_path)

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
                # Kling variant is viewable immediately — mark the clip completed
                # so it shows right away, no waiting for the Flow/Veo pass. The
                # normal worker is NOT suppressed by this: its v848 submit guard
                # (clip_done_in_platform require_approved=True) skips only APPROVED
                # clips, so an uploaded-but-unapproved Kling render still lets Flow
                # generate its own variant alongside.
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


@app.get("/api/user-worker/download/autoedit/{name:path}")
async def serve_autoedit_worker_file(name: str):
    """Serve the auto-edit worker and the modules it imports.

    v938.24 — the auto-edit worker was operator-only: the one launcher that
    existed hard-coded `c:\\Users\\tomma\\Documents\\Videos Obsidian 2`, so
    nobody else could run it and every finish fell back to the server, where
    burning captions takes 20-30 minutes instead of 3-4.

    Unlike flow_worker.py this is not ONE file — the worker imports its
    pipeline from the repo — so the whole set is served here and the installer
    lays it out in the same shape the imports expect:

        <dir>/                    autoedit_pipeline.py, send_to_platform.py, ...
        <dir>/static/             autoedit_worker.py
        <dir>/caption_templates/  the korella style

    The allow-list is explicit. A path parameter that reaches the filesystem is
    a directory-traversal hole otherwise, and `..` in a URL survives more
    normalisation than people expect.
    """
    ALLOWED = {
        "autoedit_worker.py": Path("static") / "autoedit_worker.py",
        "autoedit_pipeline.py": Path("autoedit_pipeline.py"),
        "autoedit_qc.py": Path("autoedit_qc.py"),
        "autoedit_captions.py": Path("autoedit_captions.py"),
        "autoedit_queue.py": Path("autoedit_queue.py"),
        "send_to_platform.py": Path("send_to_platform.py"),
        "measure_capcut_match.py": Path("measure_capcut_match.py"),
        "audio_processor.py": Path("audio_processor.py"),
        "config.py": Path("config.py"),
        # the korella house style — without these the worker still runs but only
        # offers pycaps' builtin looks, not ours
        "korella/pycaps.template.json": Path("caption_templates/korella/pycaps.template.json"),
        "korella/styles.css": Path("caption_templates/korella/styles.css"),
        "korella/Montserrat-ExtraBold.ttf":
            Path("caption_templates/korella/resources/Montserrat-ExtraBold.ttf"),
    }
    rel = ALLOWED.get(name)
    if rel is None:
        raise HTTPException(404, f"not part of the auto-edit worker: {name}")
    path = Path(__file__).parent / rel
    if not path.exists():
        raise HTTPException(404, f"missing on the server: {name}")
    # The font is BINARY. read_text() on it either throws or silently mangles
    # the bytes, and a corrupted font fails later inside a browser render where
    # the message says nothing about fonts.
    media = {".json": "application/json", ".css": "text/css",
             ".ttf": "font/ttf"}.get(path.suffix, "text/x-python")
    if path.suffix == ".ttf":
        return Response(content=path.read_bytes(), media_type=media)
    return Response(content=path.read_text(encoding="utf-8"), media_type=media)


@app.get("/api/user-worker/download/autoedit-installer")
async def download_autoedit_installer(
    request: Request,
    os: str = Query("windows", regex="^(windows|mac|linux)$"),
    user: User = Depends(get_current_user),
    db: DBSession = Depends(get_db_session),
):
    """One .bat that sets up the auto-edit worker on the caller's own PC.

    It reuses the token the main worker installer already wrote to
    %USERPROFILE%\\veo-worker\\.env — `resolve_token()` reads that file — so
    there is nothing to paste and no second token to manage. If that file is
    missing the installer says to run the main worker setup first rather than
    failing halfway through.
    """
    token = db.query(UserWorkerToken).filter(
        UserWorkerToken.user_id == user.id,
        UserWorkerToken.is_active == True  # noqa: E712
    ).first()
    if not token:
        token = UserWorkerToken(
            id=secrets.token_urlsafe(48),
            user_id=user.id,
            name=f"AutoEdit-{datetime.utcnow().strftime('%Y%m%d-%H%M')}",
        )
        db.add(token)
        db.commit()

    app_url = str(request.base_url).rstrip('/')
    if 'kavenobuilder.com' not in app_url:
        app_url = "https://kavenobuilder.com"

    if os == "windows":
        return Response(
            content=_generate_autoedit_installer(token.id, app_url),
            media_type="application/x-bat",
            headers={"Content-Disposition": 'attachment; filename="Kaveno-AutoEdit-Setup.bat"'},
        )
    # .command so macOS runs it on double-click; Linux users run it with bash.
    return Response(
        content=_generate_autoedit_installer_unix(token.id, app_url),
        media_type="application/x-sh",
        headers={"Content-Disposition": 'attachment; filename="Kaveno-AutoEdit-Setup.command"'},
    )


@app.get("/api/user-worker/download/gemini_video_worker.py")
async def serve_gemini_video_worker():
    """Serve the EMERGENCY worker — renders clips through the Gemini web app when
    flow_worker.py cannot run. Claims the same /api/user-worker job queue.
    Imports worker_profile_pull.py, which is served by the endpoint below."""
    worker_path = Path(__file__).parent / "static" / "gemini_video_worker.py"
    if not worker_path.exists():
        raise HTTPException(404, "Emergency worker script not found")
    return Response(content=worker_path.read_text(encoding="utf-8"),
                    media_type="text/x-python")


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


@app.get("/api/user-worker/download/browser_driver.py")
async def serve_browser_driver():
    """Serve the browser-driver companion module. flow_worker.py imports it at
    module load to pick Patchright (Chrome) vs Camoufox (Firefox), so a worker
    that cannot fetch this file cannot start at all. The worker's auto-updater
    fetches it next to flow_worker.py every launch."""
    mod_path = Path(__file__).parent / "static" / "browser_driver.py"
    if not mod_path.exists():
        raise HTTPException(404, "Module not found")
    return Response(content=mod_path.read_text(), media_type="text/x-python")


@app.get("/api/user-worker/download/firefox_profile_pull.py")
async def serve_firefox_profile_pull():
    """Serve the browser-driver companion module. flow_worker.py imports it at
    module load to pick Patchright (Chrome) vs Camoufox (Firefox), so a worker
    that cannot fetch this file cannot start at all. The worker's auto-updater
    fetches it next to flow_worker.py every launch."""
    mod_path = Path(__file__).parent / "static" / "firefox_profile_pull.py"
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

    # v899 — last_seen alone lies. It is refreshed by ANY call on this token, and
    # the operator runs image_worker.py / chatgpt_image_worker.py on the SAME
    # token, so a stopped Flow worker kept showing Online. Prefer the Flow-worker
    # heartbeat when we have one; fall back to last_seen only for workers old
    # enough not to send a worker_id (never let 'unknown' read as offline).
    _flow_live = flow_worker_online(user.id)
    if _flow_live is not None:
        online = _flow_live
    
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


@app.get("/api/user-worker/download/emergency-installer")
async def download_emergency_installer(
    request: Request,
    os: str = Query("windows", regex="^(windows|mac|linux)$"),
    laptop_email: str = Query(""),
    user: User = Depends(get_current_user),
    db: DBSession = Depends(get_db_session)
):
    """Installer for the EMERGENCY (Gemini) worker — same download-and-run shape
    as the Flow installer, with the token and the operator's Gmail baked in.

    laptop_email is the startup setting that matters here: the worker copies that
    account's live session out of a non-stable Chrome channel, so there is no
    manual login.
    """
    _email = (laptop_email or "").strip()
    if len(_email) > 254 or not _re.match(
            r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$', _email):
        raise HTTPException(400, "A valid Google account email is required")

    token = db.query(UserWorkerToken).filter(
        UserWorkerToken.user_id == user.id,
        UserWorkerToken.is_active == True  # noqa: E712
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
        return Response(
            content=_generate_emergency_installer_windows(token.id, app_url, _email),
            media_type="application/x-bat",
            headers={
                "Content-Disposition": "attachment; filename=KavenoBuilder-Emergency-Worker.bat",
                "Cache-Control": "no-store",
            },
        )
    # Mac/Linux: zip the .command so the execute bit survives the download
    import zipfile, io, time as _t
    content = _generate_emergency_installer_unix(token.id, app_url, _email)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        info = zipfile.ZipInfo("KavenoBuilder-Emergency-Worker.command",
                               date_time=_t.localtime()[:6])
        info.external_attr = 0o755 << 16
        info.create_system = 3
        zf.writestr(info, content)
    return Response(
        content=buf.getvalue(),
        media_type="application/zip",
        headers={
            "Content-Disposition": "attachment; filename=KavenoBuilder-Emergency-Worker.zip",
            "Cache-Control": "no-store",
        },
    )


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


def _generate_emergency_installer_windows(token: str, app_url: str, email: str) -> str:
    """Windows .bat for the EMERGENCY (Gemini) worker.

    Same shape as the Flow installer: token baked in, the operator's Gmail baked
    in as the startup setting, its own folder, and a start script it can re-run.
    ASCII ONLY — a non-ASCII char in a .bat/.ps1 breaks parsing on PS 5.1.
    """
    return f'''@echo off
setlocal enabledelayedexpansion
title KavenoBuilder Emergency Worker
mode con: cols=64 lines=26
color 6F

echo.
echo   KavenoBuilder - EMERGENCY Worker (Gemini)
echo.
echo   Use this only when the normal worker cannot run.
echo   It claims the SAME jobs, so STOP the normal worker first.
echo.

set "WORKER_DIR=%USERPROFILE%\\kaveno-gemini-worker"
mkdir "%WORKER_DIR%" 2>nul

where python >nul 2>nul
if errorlevel 1 (
  echo   [X] Python not found. Install Python 3 first, then re-run this file.
  pause
  exit /b 1
)

echo   Installing browser automation (one time, may take a minute)...
python -m pip install --quiet --upgrade patchright >nul 2>nul
python -m patchright install chromium >nul 2>nul
echo   [OK] Ready

echo   Downloading the emergency worker...
powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '{app_url}/api/user-worker/download/gemini_video_worker.py' -OutFile '%WORKER_DIR%\\gemini_video_worker.py' -UseBasicParsing" >nul 2>nul
powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '{app_url}/api/user-worker/download/worker_profile_pull.py' -OutFile '%WORKER_DIR%\\worker_profile_pull.py' -UseBasicParsing" >nul 2>nul
echo   [OK] Downloaded

> "%WORKER_DIR%\\.env" (
echo USER_WORKER_TOKEN={token}
echo WEB_APP_URL={app_url}
echo GEMINI_EMAIL={email}
)

> "%WORKER_DIR%\\start-emergency-worker.bat" (
echo @echo off
echo title KavenoBuilder Emergency Worker
echo cd /d "%%~dp0"
echo python gemini_video_worker.py --email {email} --serve --token {token}
echo pause
)

echo.
echo   Account: {email}
echo   Folder : %WORKER_DIR%
echo.
echo   NOTE: that Google account must be signed into CHROME BETA.
echo   The worker copies that session; your daily Chrome is never touched.
echo.
echo   Starting the emergency worker now...
echo   (re-run start-emergency-worker.bat in the folder above any time)
echo.
cd /d "%WORKER_DIR%"
python gemini_video_worker.py --email {email} --serve --token {token}
pause
'''


def _generate_emergency_installer_unix(token: str, app_url: str, email: str) -> str:
    """Mac/Linux .command counterpart of the emergency installer."""
    return f'''#!/bin/bash
set -e
echo ""
echo "  KavenoBuilder - EMERGENCY Worker (Gemini)"
echo ""
echo "  Use this only when the normal worker cannot run."
echo "  It claims the SAME jobs, so STOP the normal worker first."
echo ""

WORKER_DIR="$HOME/kaveno-gemini-worker"
mkdir -p "$WORKER_DIR"
cd "$WORKER_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "  [X] python3 not found. Install Python 3 first, then re-run."
  read -n 1 -s -r -p "  Press any key to close"
  exit 1
fi

echo "  Installing browser automation (one time)..."
python3 -m pip install --quiet --upgrade patchright >/dev/null 2>&1 || true
python3 -m patchright install chromium >/dev/null 2>&1 || true
echo "  [OK] Ready"

echo "  Downloading the emergency worker..."
curl -sL "{app_url}/api/user-worker/download/gemini_video_worker.py" -o gemini_video_worker.py
curl -sL "{app_url}/api/user-worker/download/worker_profile_pull.py" -o worker_profile_pull.py
echo "  [OK] Downloaded"

cat > .env <<EOF
USER_WORKER_TOKEN={token}
WEB_APP_URL={app_url}
GEMINI_EMAIL={email}
EOF

cat > start-emergency-worker.command <<EOF
#!/bin/bash
cd "\\$(dirname "\\$0")"
python3 gemini_video_worker.py --email {email} --serve --token {token}
EOF
chmod +x start-emergency-worker.command

echo ""
echo "  Account: {email}"
echo "  Folder : $WORKER_DIR"
echo ""
echo "  NOTE: that Google account must be signed into a NON-STABLE Chrome"
echo "  channel (Beta/Dev/Canary). Your daily Chrome is never touched."
echo ""
echo "  Starting the emergency worker now..."
echo ""
python3 gemini_video_worker.py --email {email} --serve --token {token}
'''


AUTOEDIT_WORKER_FILES = [
    ("autoedit_worker.py", "static"),
    ("autoedit_pipeline.py", ""),
    ("autoedit_qc.py", ""),
    ("autoedit_captions.py", ""),
    ("autoedit_queue.py", ""),
    ("send_to_platform.py", ""),
    ("measure_capcut_match.py", ""),
    ("audio_processor.py", ""),
    ("config.py", ""),
    ("korella/pycaps.template.json", "caption_templates\\korella"),
    ("korella/styles.css", "caption_templates\\korella"),
    ("korella/Montserrat-ExtraBold.ttf", "caption_templates\\korella\\resources"),
]


def _generate_autoedit_installer_unix(token: str, app_url: str) -> str:
    """Mac / Linux twin of the Windows auto-edit installer.

    v938.24b. Deliberately built with __PLACEHOLDER__ replacement rather than an
    f-string: this is dense bash, and `${VAR}` inside an f-string has to be
    written `${{VAR}}` everywhere. One missed pair produces a script that looks
    right in review and expands to nonsense at run time.

    Same contract as the Windows one — reuse the token the main installer wrote,
    check the hard prerequisites first, lay the files out in the shape the
    imports expect, leave a start script behind.
    """
    fetches = "\n".join(
        'curl -sfL "$APP/api/user-worker/download/autoedit/%s" -o "$AE/%s%s" || fail_dl'
        % (fn, (sub.replace("\\", "/") + "/") if sub else "", fn.rsplit("/", 1)[-1])
        for fn, sub in AUTOEDIT_WORKER_FILES
    )
    script = r'''#!/bin/bash
# Kaveno Auto-Edit worker setup (Mac / Linux).
#
# Finishes videos on THIS machine instead of the server. The server can do it
# too, but it has one slow cpu: burning the captions takes 20-30 minutes there
# and 3-4 minutes here.

APP="__APP__"
DIR="$HOME/veo-worker"
AE="$DIR/autoedit"

fail_dl() { echo ""; echo "  [X] a download failed. Check the connection and run this again."; exit 1; }

echo ""
echo "  Kaveno Auto-Edit worker"
echo "  ======================="
echo ""

PY=""
for c in python3 python; do command -v "$c" >/dev/null 2>&1 && { PY="$c"; break; }; done
if [ -z "$PY" ]; then
  echo "  [X] python3 is not installed. Install it, then run this again."
  exit 1
fi

if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "  [X] ffmpeg is not on PATH - the worker cannot render without it."
  echo "      Mac:   brew install ffmpeg"
  echo "      Linux: sudo apt install ffmpeg"
  exit 1
fi

if [ ! -f "$DIR/.env" ]; then
  echo "  [X] $DIR/.env not found."
  echo "      Run the main worker setup from the My Worker page first - this"
  echo "      reuses the token it writes, so there is nothing to paste here."
  exit 1
fi

echo "  [1/4] making $AE"
mkdir -p "$AE/static" "$AE/caption_templates/korella/resources"

echo "  [2/4] downloading the worker"
__FETCHES__

echo "  [3/4] python packages (a few minutes the first time)"
"$PY" -m pip install --quiet --upgrade pip >/dev/null 2>&1
"$PY" -m pip install --quiet requests numpy "opencv-python<5" || {
  echo "  [X] installing the python packages failed - the message above says why."
  exit 1
}
echo "        optional: pycaps gives the TikTok-style captions. Without it the"
echo "        worker still runs and uses a plainer caption look."
"$PY" -m pip install --quiet "pycaps @ git+https://github.com/francozanardi/pycaps" playwright openai-whisper >/dev/null 2>&1 \
  && "$PY" -m playwright install chromium >/dev/null 2>&1

echo "  [4/4] writing the start script"
cat > "$DIR/start-autoedit.command" <<'LAUNCHER'
#!/bin/bash
cd "$(dirname "$0")"
set -a; . ./.env; set +a
export PYTHONIOENCODING=utf-8
echo "Watching for videos to finish. Leave this window open."
PY=""
for c in python3 python; do command -v "$c" >/dev/null 2>&1 && { PY="$c"; break; }; done
"$PY" autoedit/static/autoedit_worker.py --watch
LAUNCHER
chmod +x "$DIR/start-autoedit.command"

echo ""
echo "  Done. Starting the worker now - leave this window open. Every"
echo "  \"Finish video\" you press in the platform is picked up here instead"
echo "  of the server. Close it to go back to normal."
echo ""
echo "  Start it again any time with:"
echo "      $DIR/start-autoedit.command"
echo ""
exec "$DIR/start-autoedit.command"
'''
    return script.replace("__APP__", app_url).replace("__FETCHES__", fetches)


def _generate_autoedit_installer(token: str, app_url: str) -> str:
    """A .bat that installs the auto-edit worker into %USERPROFILE%\\veo-worker\\autoedit.

    v938.24. Kept deliberately plain: fetch the files, install the python deps,
    write a start script, run it. No scheduled task, no service — the operator
    starts it when they want their PC to take the work.

    Layout matters. autoedit_worker.py does `sys.path.insert(parent.parent)`
    and imports its pipeline as a sibling, so the files go into the same shape
    the repo has or the imports fail with nothing useful to say.

    ffmpeg is checked FIRST and the script stops if it is missing. Everything
    downstream needs it, and a failure 200 lines later reads as "the worker is
    broken" instead of "install ffmpeg".
    """
    # The URL name can carry a prefix ("korella/styles.css") to keep the
    # allow-list unambiguous; what lands on disk is always the BASENAME inside
    # its own folder, or the file would be written as "korella/styles.css"
    # under an already-korella directory.
    fetches = "\n".join(
        'curl -sfL "%%APP%%/api/user-worker/download/autoedit/%s" -o "%%AE%%\\%s%s" || goto :dlfail'
        % (fn, (sub + "\\") if sub else "", fn.rsplit("/", 1)[-1])
        for fn, sub in AUTOEDIT_WORKER_FILES
    )
    return f"""@echo off
setlocal EnableDelayedExpansion
title Kaveno Auto-Edit worker setup
set "APP={app_url}"
set "WORKER_DIR=%USERPROFILE%\\veo-worker"
set "AE=%WORKER_DIR%\\autoedit"

echo.
echo   Kaveno Auto-Edit worker
echo   =======================
echo   Finishes videos on THIS pc instead of the server.
echo   The server can do it too, but it has one slow cpu: burning the
echo   captions takes 20-30 minutes there and 3-4 minutes here.
echo.

where python >nul 2>nul || (echo   [X] python is not installed or not on PATH. && echo       Install it from python.org and tick "Add to PATH". && pause && exit /b 1)
where ffmpeg >nul 2>nul || (echo   [X] ffmpeg is not on PATH - the worker cannot render without it. && echo       Get it from https://www.gyan.dev/ffmpeg/builds/ and add the bin folder to PATH. && pause && exit /b 1)

if not exist "%WORKER_DIR%\\.env" (
  echo   [X] %WORKER_DIR%\\.env not found.
  echo       Run the main worker setup from the My Worker page first - this
  echo       reuses the token it writes, so there is nothing to paste here.
  pause
  exit /b 1
)

echo   [1/4] making %AE%
mkdir "%AE%" 2>nul
mkdir "%AE%\\static" 2>nul
mkdir "%AE%\\caption_templates" 2>nul
mkdir "%AE%\\caption_templates\\korella" 2>nul
mkdir "%AE%\\caption_templates\\korella\\resources" 2>nul

echo   [2/4] downloading the worker
{fetches}

echo   [3/4] python packages (a few minutes the first time)
python -m pip install --quiet --upgrade pip >nul 2>nul
python -m pip install --quiet requests numpy "opencv-python<5" || goto :pipfail
echo         optional: pycaps gives the TikTok-style captions. Without it the
echo         worker still runs and uses a plainer caption look.
python -m pip install --quiet "pycaps @ git+https://github.com/francozanardi/pycaps" playwright openai-whisper >nul 2>nul && python -m playwright install chromium >nul 2>nul

echo   [4/4] writing the start script
> "%WORKER_DIR%\\start-autoedit.bat" echo @echo off
>> "%WORKER_DIR%\\start-autoedit.bat" echo title Auto-Edit worker - leave this open
>> "%WORKER_DIR%\\start-autoedit.bat" echo cd /d "%%~dp0"
>> "%WORKER_DIR%\\start-autoedit.bat" echo for /f "usebackq tokens=1,* delims==" %%%%a in (".env") do set "%%%%a=%%%%b"
>> "%WORKER_DIR%\\start-autoedit.bat" echo set PYTHONIOENCODING=utf-8
>> "%WORKER_DIR%\\start-autoedit.bat" echo echo Watching for videos to finish. Leave this window open.
>> "%WORKER_DIR%\\start-autoedit.bat" echo python "autoedit\\static\\autoedit_worker.py" --watch
>> "%WORKER_DIR%\\start-autoedit.bat" echo pause

echo.
echo   Done. Starting the worker in its own window now - leave that window
echo   open. Every "Finish video" you press in the platform is picked up
echo   there instead of the server. Close it to go back to normal.
echo.
echo   Start it again any time with:
echo       %WORKER_DIR%\\start-autoedit.bat
echo.
start "" "%WORKER_DIR%\\start-autoedit.bat"
exit /b 0

:dlfail
echo.
echo   [X] a download failed. Check the connection and run this again.
pause
exit /b 1

:pipfail
echo.
echo   [X] installing the python packages failed - the message above says why.
pause
exit /b 1
"""


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
powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '%APP_URL%/api/user-worker/download/flow_attribution.py' -OutFile '%WORKER_DIR%\\flow_attribution.py' -UseBasicParsing" >nul 2>nul
echo         OK

echo   [4/5] Writing config...
(
echo WORKER_MODE=user
echo USER_WORKER_TOKEN=%TOKEN%
echo WEB_APP_URL=%APP_URL%
echo SESSION_FOLDER=%WORKER_DIR%\\chrome-session
echo DOWNLOAD_SESSION_FOLDER=%WORKER_DIR%\\chrome-download
REM Firefox (Camoufox) is the default: Chrome 151+ mints reCAPTCHA tokens Flow
REM refuses (~0%% real as of 2026-08-07) while Firefox minted 10/10 accepted.
REM Set BROWSER_MODE=stealth to fall back to Chrome. The SESSION_FOLDER above
REM only applies in single-account mode; multi-account derives per-engine dirs
REM (firefox-session / chrome-session) so the two browsers never share a profile.
echo BROWSER_MODE=firefox
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
curl -sL "{app_url}/api/user-worker/download/flow_attribution.py" -o "$DIR/flow_attribution.py"
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
# Firefox (Camoufox) is the default: Chrome 151+ mints reCAPTCHA tokens Flow
# refuses (~0% real as of 2026-08-07) while Firefox minted 10/10 accepted.
# Set BROWSER_MODE=stealth to fall back to Chrome. SESSION_FOLDER above only
# applies in single-account mode; multi-account derives per-engine dirs
# (firefox-session / chrome-session) so the two browsers never share a profile.
BROWSER_MODE=firefox
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
