# -*- coding: utf-8 -*-
"""
Database models for Veo Web App
Uses SQLAlchemy with SQLite (easily upgradeable to PostgreSQL)
"""

import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime,
    Boolean, Float, ForeignKey, Enum as SQLEnum, JSON, event, Index
)
from sqlalchemy.exc import OperationalError as SQLAOperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.pool import NullPool, StaticPool
from contextlib import contextmanager
import logging
import time as _time

from config import (
    JobStatus, ClipStatus, ErrorCode, app_config,
    AspectRatio, Resolution, Duration, PersonGeneration
)

Base = declarative_base()


def _ig_thumb_public_url(video_id, thumb_r2_key, raw_thumb_url):
    from instagram_client import thumb_public_url
    return thumb_public_url(video_id, thumb_r2_key, raw_thumb_url)


class User(Base):
    """User table - stores Google OAuth users"""
    __tablename__ = "users"
    
    id = Column(String(36), primary_key=True)  # UUID
    email = Column(String(255), unique=True, nullable=False)
    name = Column(String(255), nullable=True)
    picture = Column(String(500), nullable=True)  # Profile picture URL
    google_id = Column(String(255), unique=True, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True)

    # v815 — per-account settings JSON blob. Currently holds
    # {"auto_image_retry": {"mode": "off|next|prev|batch"}}. Nullable;
    # absent = defaults applied in code.
    settings_json = Column(Text, nullable=True)

    # Relationships
    jobs = relationship("Job", back_populates="user", cascade="all, delete-orphan")
    sessions = relationship("UserSession", back_populates="user", cascade="all, delete-orphan")
    api_keys = relationship("UserAPIKey", back_populates="user", cascade="all, delete-orphan")
    worker_tokens = relationship("UserWorkerToken", back_populates="user", cascade="all, delete-orphan")
    instagram_accounts = relationship(
        "InstagramAccount", back_populates="user", cascade="all, delete-orphan"
    )
    drive_accounts = relationship(
        "DriveAccount", back_populates="user", cascade="all, delete-orphan"
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "email": self.email,
            "name": self.name,
            "picture": self.picture,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "last_login": self.last_login.isoformat() if self.last_login else None,
        }


class UserSession(Base):
    """Session table - stores auth sessions"""
    __tablename__ = "user_sessions"
    
    id = Column(String(64), primary_key=True)  # Session token
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    user = relationship("User", back_populates="sessions")


class UserAPIKey(Base):
    """User's personal API keys for Gemini/Veo"""
    __tablename__ = "user_api_keys"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    
    # Key info
    key_value = Column(String(255), nullable=False)  # The actual API key
    key_name = Column(String(100), nullable=True)    # Optional friendly name
    key_suffix = Column(String(10), nullable=True)   # Last 6 chars for display
    
    # Status
    is_valid = Column(Boolean, default=True)         # Whether key is valid
    is_active = Column(Boolean, default=True)        # Whether user wants to use it
    key_status = Column(String(20), default="unknown")  # working, rate_limited, invalid, unknown
    last_used = Column(DateTime, nullable=True)
    last_error = Column(Text, nullable=True)
    last_checked = Column(DateTime, nullable=True)   # When status was last validated
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    user = relationship("User", back_populates="api_keys")
    
    def to_dict(self, hide_key: bool = True) -> Dict[str, Any]:
        return {
            "id": self.id,
            "key_name": self.key_name or f"Key ...{self.key_suffix}",
            "key_suffix": self.key_suffix,
            "key_preview": f"...{self.key_suffix}" if hide_key else self.key_value,
            "is_valid": self.is_valid,
            "is_active": self.is_active,
            "key_status": self.key_status or ("working" if self.is_valid else "invalid"),
            "last_used": self.last_used.isoformat() if self.last_used else None,
            "last_error": self.last_error,
            "last_checked": self.last_checked.isoformat() if self.last_checked else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class UserWorkerToken(Base):
    """Tokens for users running their own local Flow workers.
    
    Each token is scoped to a single user and allows their worker
    to only see/process their own jobs.
    """
    __tablename__ = "user_worker_tokens"
    
    id = Column(String(64), primary_key=True)  # The token itself (secrets.token_urlsafe(48))
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    name = Column(String(100), default="My Worker")  # Friendly name
    
    created_at = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, nullable=True)  # Updated on each API call
    is_active = Column(Boolean, default=True)
    
    # Relationship
    user = relationship("User", back_populates="worker_tokens")
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id[:8] + "..." + self.id[-4:],  # Don't expose full token in lists
            "name": self.name,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "is_active": self.is_active,
        }


class Job(Base):
    """Main job table - one job = one video generation request"""
    __tablename__ = "jobs"
    
    id = Column(String(36), primary_key=True)  # UUID
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True)  # Owner
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Status
    status = Column(String(20), default=JobStatus.PENDING.value)
    progress_percent = Column(Float, default=0.0)
    
    # Configuration (stored as JSON for flexibility)
    config_json = Column(Text, nullable=False)
    
    # Dialogue lines (stored as JSON)
    dialogue_json = Column(Text, nullable=False)
    
    # API keys (encrypted in production!)
    api_keys_json = Column(Text, nullable=True)  # Should encrypt in production
    
    # Statistics
    total_clips = Column(Integer, default=0)
    completed_clips = Column(Integer, default=0)
    failed_clips = Column(Integer, default=0)
    skipped_clips = Column(Integer, default=0)
    
    # Timing
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    # Upload paths
    images_dir = Column(String(500), nullable=False)
    output_dir = Column(String(500), nullable=False)
    
    # === NEW: Flow Backend Fields ===
    backend = Column(String(20), default="api")  # 'api' or 'flow'
    flow_project_url = Column(String(500), nullable=True)  # Flow project URL
    flow_state_json = Column(Text, nullable=True)  # Flow automation state
    flow_needs_auth = Column(Boolean, default=False)  # Auth required flag
    
    # === Cloud Storage for Frames ===
    # Stores R2/S3 keys for uploaded frames (JSON: {"image_00.jpg": "jobs/xxx/frames/image_00.jpg", ...})
    # This allows redos to work even if local ephemeral storage is cleared
    frames_storage_keys = Column(Text, nullable=True)  # JSON dict of filename -> R2 key
    
    # === Lineup Override ===
    # When set, export/assembly uses this ordered list of clip IDs instead of ORDER BY clip_index
    # Format: JSON array of clip IDs, e.g. [5, 3, 7, 1]
    # null = default order (ORDER BY clip_index)
    clip_order_json = Column(Text, nullable=True)
    
    # === Export / Voice Clone Tracking ===
    has_export = Column(Boolean, default=False)       # True after successful video export
    has_voice_clone = Column(Boolean, default=False)   # True after successful voice swap
    
    # === Worker Claiming Fields ===
    claimed_by_worker = Column(String(100), nullable=True)  # Worker ID that claimed this job
    claimed_at = Column(DateTime, nullable=True)  # When the job was claimed
    
    # v455: abort signal. Set True when user clicks Delete on a job that's
    # actively being processed. Worker learns about it via heartbeat
    # response and exits the processing loop cleanly before we actually
    # delete the DB row.
    abort_requested = Column(Boolean, default=False, nullable=True)
    
    # === Post-render lifecycle tracker (2026-05-29) ===
    # See docs/superpowers/specs/2026-05-29-video-lifecycle-tracker-design.md.
    # NULL while Job is pre-completion. Auto-entered on COMPLETED + has_export.
    lifecycle_stage = Column(String(32), nullable=True, default=None)
    approval_at = Column(DateTime, nullable=True)
    export_at = Column(DateTime, nullable=True)
    finishing_at = Column(DateTime, nullable=True)
    published_at = Column(DateTime, nullable=True)
    notes = Column(Text, nullable=True)
    archived = Column(Boolean, nullable=False, default=False)

    # === Instagram link (2026-05-31) ===
    instagram_url       = Column(String(500), nullable=True)
    instagram_video_id  = Column(Integer, ForeignKey("instagram_videos.id"), nullable=True)

    # === How the job reached `published` lifecycle stage (2026-06-01) ===
    # 'drive_watch' = Google Drive folder watcher matched a local final-cut
    #   file against this job's dialogue (advanced before IG post lands).
    # 'ig_match'    = Instagram sync transcribed a posted reel and matched
    #   it against this job's dialogue (the only path pre-Drive feature).
    # 'manual'      = operator clicked the Published advance button.
    # NULL          = legacy / pre-feature jobs.
    published_via = Column(String(20), nullable=True)

    # Relationships
    user = relationship("User", back_populates="jobs")
    clips = relationship("Clip", back_populates="job", cascade="all, delete-orphan")
    logs = relationship("JobLog", back_populates="job", cascade="all, delete-orphan")
    blacklist = relationship("BlacklistEntry", back_populates="job", cascade="all, delete-orphan")
    
    def to_dict(self) -> Dict[str, Any]:
        # For UI display, translate internal statuses to user-friendly ones
        display_status = self.status
        if self.status == "queued_for_flow":
            display_status = "pending"  # Show as pending in UI
        
        return {
            "id": self.id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "status": display_status,
            "progress_percent": self.progress_percent,
            "total_clips": self.total_clips,
            "completed_clips": self.completed_clips,
            "failed_clips": self.failed_clips,
            "skipped_clips": self.skipped_clips,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "config": json.loads(self.config_json) if self.config_json else {},
            "dialogue_count": len(json.loads(self.dialogue_json)) if self.dialogue_json else 0,
            # Flow backend fields
            "backend": self.backend or "api",
            "flow_project_url": self.flow_project_url,
            # Lineup override
            "has_lineup_override": getattr(self, 'clip_order_json', None) is not None,
            # Drive-watch lifecycle path (2026-06-01)
            "published_via": getattr(self, "published_via", None),
        }


class Clip(Base):
    """Individual clip within a job"""
    __tablename__ = "clips"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(36), ForeignKey("jobs.id"), nullable=False)
    
    # Clip identification
    clip_index = Column(Integer, nullable=False)  # 0-based index
    dialogue_id = Column(Integer, nullable=False)  # ID from dialogue line
    dialogue_text = Column(Text, nullable=False)
    # v644 — optional audio-padding suffix appended to the Veo prompt's
    # dialogue. Veo 3.1's experimental audio path (especially on Fast
    # Lower Priority tier) frequently fails on short lines (≤9 words)
    # with "Audio generation failed" at the 27% checkpoint. Pad brings
    # the spoken text up to ~20 words. Whisper-VAD continues to use
    # `dialogue_text` (the bare line) as script truth, so the pad's
    # spoken audio is automatically trimmed by the existing apply_vad
    # pipeline as unmatched filler. Nullable; absent = no padding,
    # Veo prompt uses bare dialogue_text.
    dialogue_pad = Column(Text, nullable=True)

    # v667/v668 — hybrid trim mode + anchor-derived durations.
    # cut_mode: 'whisper' (default; existing apply_vad path) | 'timeline'
    # (anchor-derived ffmpeg trim, skip whisper-VAD) | NULL → whisper.
    # target_duration_s: derived at storyboard build from
    # image_N.frame_anchor_s vs the next image's anchor. NULL on pre-v667
    # clips; whisper path ignores it.
    # veo_render_duration_s: ceil_to(target_duration_s, [4,6,8]); used to
    # pick the Veo render bucket for transformation chains. NULL = no
    # override (existing default duration logic applies).
    cut_mode = Column(String(20), nullable=True)
    target_duration_s = Column(Float, nullable=True)
    veo_render_duration_s = Column(Integer, nullable=True)

    # v681 — text-card / caption denorm from ImageSceneAssignment.
    # caption: source caption (informational on shot clips) OR the
    # rendered text on text_card clips.
    # scene_type: 'shot' | 'text_card' | NULL → shot.
    # bg_color: solid color hex / CSS name used by ffmpeg drawtext for
    # text_card clips. NULL on every legacy clip row.
    caption = Column(Text, nullable=True)
    scene_type = Column(String(20), nullable=True)
    bg_color = Column(String(20), nullable=True)

    # v698A — per-scene clip-pair for voiceover-over-b-roll.
    # clip_role: 'single' (default — current behavior) | 'visual_pair' (b-roll
    #   silent visual that pairs with an audio twin) | 'audio_pair' (audio-only
    #   source clip whose visual is discarded at export and audio is overlaid
    #   onto the paired visual_pair).
    # paired_clip_id: bidirectional FK between visual_pair and audio_pair rows.
    # voiceover_anchor_image_node_id: (on visual_pair only) which ImageNode the
    #   audio twin is rendered from — must be a persona-on-camera image with
    #   role=voiceover_anchor.
    # voiceover_line: (on visual_pair only) the line spoken by the audio twin.
    #   Stored on the visual side so we can re-derive the audio job if a clip
    #   gets deleted/recreated downstream.
    clip_role = Column(String(20), nullable=True)
    paired_clip_id = Column(Integer, ForeignKey("clips.id"), nullable=True)
    voiceover_anchor_image_node_id = Column(Integer, ForeignKey("image_nodes.id"), nullable=True)
    voiceover_line = Column(Text, nullable=True)
    # v718i (NEW 2026-05-18): per-clip explicit end-frame image binding for
    # v718h-C Option C Veo native end-frame interpolation. When the Scene
    # block carries an `- **end_frame_image:** image_K+1` bullet, the
    # platform binds the named ImageNode here. veo_generator.py:2605
    # uses this (when set) for cfg.last_frame instead of auto-inferring
    # from the next clip's start frame. Halves Veo render cost on
    # structural-axis HOOKs vs v718h-B Multi-Clip Blend (1 clip vs 2).
    # NULL = sequential auto-inference (legacy default).
    # Migration registered in image_platform.py (mirrors voiceover_anchor_image_node_id).
    end_frame_image_node_id = Column(Integer, ForeignKey("image_nodes.id"), nullable=True)

    # Status
    status = Column(String(20), default=ClipStatus.PENDING.value)
    retry_count = Column(Integer, default=0)
    
    # Frame info
    start_frame = Column(String(255), nullable=True)
    end_frame = Column(String(255), nullable=True)
    # v701 — when Flow rejects start_frame for content-policy reasons,
    # the worker stamps `error_code='CONTENT_POLICY_VIOLATION'` and the
    # original rejected frame R2 key gets stashed here so the frontend
    # can show it back to the user. After the user uploads a replacement,
    # `start_frame` is rewritten to the new key and `replacement_start_frame`
    # carries the audit trail of the previous rejected frame(s).
    replacement_start_frame = Column(String(512), nullable=True)

    # v815 — auto-image-retry audit. JSON: {original_frame, used_frame,
    # tried:[...], count, mode}. Set when prominent-people auto-retry swaps
    # the start_frame. Drives the "image rejected -> used image X" card mark.
    auto_image_retry_json = Column(Text, nullable=True)

    # === Storyboard/Scene Mode Fields ===
    clip_mode = Column(String(20), default="fresh")  # v782 default fresh (was blend) | 'blend' | 'continue' | 'fresh'
    scene_index = Column(Integer, default=0)  # Which scene this clip belongs to
    
    # Generation parameters (for regeneration)
    prompt_text = Column(Text, nullable=True)
    # v805 — operator-authored policy-fallback prompt (Prompt B, voice-only).
    # Shipped verbatim to the worker; used only when the primary prompt trips
    # a generation-policy block (flow_worker retries same-model with this).
    prompt_text_b = Column(Text, nullable=True)
    
    # Output
    output_filename = Column(String(500), nullable=True)
    
    # === NEW: Flow Backend Fields ===
    flow_clip_id = Column(String(255), nullable=True)  # ID from Flow UI
    output_url = Column(String(500), nullable=True)  # S3/R2 URL for output
    
    # Timing
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    
    # Error info
    error_code = Column(String(50), nullable=True)
    error_message = Column(Text, nullable=True)
    
    # === NEW: Approval & Redo System ===
    approval_status = Column(String(20), default="pending_review")  # pending_review, approved, rejected, max_attempts
    generation_attempt = Column(Integer, default=1)  # 1, 2, or 3 (max)
    redo_reason = Column(Text, nullable=True)  # User's reason for requesting redo
    use_logged_params = Column(Boolean, default=True)  # True for attempt 2, False for attempt 3
    
    # History of all generated versions (JSON array)
    # Format: [{"attempt": 1, "filename": "...", "generated_at": "...", "approved": false}, ...]
    versions_json = Column(Text, default="[]")
    
    # Currently selected variant (1-based, matches attempt number)
    selected_variant = Column(Integer, default=1)

    # Kling (Higgsfield) additional-variant generation status.
    # None = not requested; 'queued'/'processing'/'done'/'failed' = server-side pass state.
    kling_variant_status = Column(String(20), nullable=True)
    
    # === Worker Claiming Fields (for redos) ===
    claimed_by_worker = Column(String(100), nullable=True)  # Worker ID that claimed this redo
    claimed_at = Column(DateTime, nullable=True)  # When the redo was claimed
    
    # Relationship
    job = relationship("Job", back_populates="clips")
    
    def to_dict(self) -> Dict[str, Any]:
        raw_versions = json.loads(self.versions_json) if self.versions_json else []
        
        # Deduplicate versions by attempt number (keep last one for each attempt)
        seen = {}
        for v in raw_versions:
            attempt = v.get("attempt")
            if attempt:
                seen[attempt] = v
        versions = sorted(seen.values(), key=lambda x: x.get("attempt", 0))
        
        # Calculate total variants from deduplicated list
        total_variants = len(versions)
        
        return {
            "id": self.id,
            "clip_index": self.clip_index,
            "dialogue_id": self.dialogue_id,
            "dialogue_text": self.dialogue_text[:100] + "..." if len(self.dialogue_text) > 100 else self.dialogue_text,
            "status": self.status,
            "retry_count": self.retry_count,
            "start_frame": self.start_frame,
            "end_frame": self.end_frame,
            "output_filename": self.output_filename,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "duration_seconds": self.duration_seconds,
            # Approval fields
            "approval_status": self.approval_status,
            "generation_attempt": self.generation_attempt,
            "redo_reason": self.redo_reason,
            "attempts_remaining": 3 - self.generation_attempt,
            # Variant fields
            "versions": versions,
            "total_variants": total_variants,
            "selected_variant": self.selected_variant or self.generation_attempt or 1,
            # v667/v668 — transformation-video metadata.
            "cut_mode": self.cut_mode,
            "target_duration_s": self.target_duration_s,
            "veo_render_duration_s": self.veo_render_duration_s,
            # v681 — text-card / caption denorm.
            "caption": self.caption,
            "scene_type": self.scene_type,
            "bg_color": self.bg_color,
        }


class JobLog(Base):
    """Log entries for a job - enables real-time streaming"""
    __tablename__ = "job_logs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(36), ForeignKey("jobs.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Log info
    level = Column(String(10), default="INFO")  # DEBUG, INFO, WARNING, ERROR
    category = Column(String(50), nullable=True)  # clip, api, system, etc.
    clip_index = Column(Integer, nullable=True)
    
    message = Column(Text, nullable=False)
    details_json = Column(Text, nullable=True)  # Extra context as JSON
    
    # Relationship
    job = relationship("Job", back_populates="logs")
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "level": self.level,
            "category": self.category,
            "clip_index": self.clip_index,
            "message": self.message,
            "details": json.loads(self.details_json) if self.details_json else None,
        }


class BlacklistEntry(Base):
    """Blacklisted images for a job"""
    __tablename__ = "blacklist"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(36), ForeignKey("jobs.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Image info
    image_filename = Column(String(255), nullable=False)
    reason = Column(String(50), nullable=True)  # celebrity_filter, generation_failed, etc.
    details = Column(Text, nullable=True)
    
    # Relationship
    job = relationship("Job", back_populates="blacklist")


class GenerationLog(Base):
    """Persistent log of generation parameters for each video"""
    __tablename__ = "generation_logs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(36), ForeignKey("jobs.id"), nullable=False)
    
    # Video identification
    video_id = Column(Integer, nullable=False)
    
    # Generation parameters
    images_dir = Column(String(500), nullable=False)
    start_frame = Column(String(255), nullable=False)
    end_frame = Column(String(255), nullable=True)
    dialogue_line = Column(Text, nullable=False)
    language = Column(String(50), nullable=False)
    prompt_text = Column(Text, nullable=False)
    video_filename = Column(String(500), nullable=False)
    aspect_ratio = Column(String(10), nullable=False)
    resolution = Column(String(10), nullable=False)
    duration = Column(String(5), nullable=False)
    
    generated_at = Column(DateTime, default=datetime.utcnow)


class InstagramAccount(Base):
    """An IG handle the operator monitors. One operator may have many."""
    __tablename__ = "instagram_accounts"

    id                = Column(Integer, primary_key=True, autoincrement=True)
    user_id           = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    handle            = Column(String(64), nullable=False)
    ig_user_id        = Column(String(32), nullable=True)
    api_key_encrypted = Column(Text, nullable=False)
    last_synced_at    = Column(DateTime, nullable=True)
    added_at          = Column(DateTime, default=datetime.utcnow)

    user   = relationship("User", back_populates="instagram_accounts")
    videos = relationship(
        "InstagramVideo", back_populates="account", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_instagram_accounts_user_handle", "user_id", "handle", unique=True),
    )

    def to_dict(self, include_counts=False):
        return {
            "id": self.id,
            "handle": self.handle,
            "ig_user_id": self.ig_user_id,
            "last_synced_at": self.last_synced_at.isoformat() if self.last_synced_at else None,
            "added_at": self.added_at.isoformat() if self.added_at else None,
        }


class InstagramVideo(Base):
    """A reel pulled from an InstagramAccount."""
    __tablename__ = "instagram_videos"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    account_id      = Column(Integer, ForeignKey("instagram_accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    shortcode       = Column(String(32), nullable=False)
    url             = Column(String(500), nullable=False)
    thumb_url       = Column(Text, nullable=True)
    thumb_r2_key    = Column(Text, nullable=True)  # R2 key of the cached thumb jpg; None = not cached yet
    video_url       = Column(Text, nullable=True)
    caption         = Column(Text, nullable=True)
    views           = Column(Integer, default=0)
    likes           = Column(Integer, default=0)
    comments        = Column(Integer, default=0)
    posted_at       = Column(DateTime, nullable=True, index=True)
    transcription   = Column(Text, nullable=True)
    transcription_status = Column(String(16), default="pending")
    transcription_error  = Column(Text, nullable=True)
    matched_job_id  = Column(String(36), ForeignKey("jobs.id"), nullable=True, index=True)
    matched_at      = Column(DateTime, nullable=True)
    created_at      = Column(DateTime, default=datetime.utcnow)

    account     = relationship("InstagramAccount", back_populates="videos")
    matched_job = relationship("Job", foreign_keys=[matched_job_id])

    __table_args__ = (
        Index("ix_instagram_videos_account_shortcode", "account_id", "shortcode", unique=True),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "shortcode": self.shortcode,
            "url": self.url,
            "thumb_url": _ig_thumb_public_url(self.id, self.thumb_r2_key, self.thumb_url),
            "video_url": self.video_url,
            "caption": self.caption,
            "views": self.views,
            "likes": self.likes,
            "comments": self.comments,
            "posted_at": self.posted_at.isoformat() if self.posted_at else None,
            "transcription_status": self.transcription_status,
            "transcription_error": self.transcription_error,
            "matched_job_id": self.matched_job_id,
        }


class DriveAccount(Base):
    """A Google Drive folder the operator watches. One per user typically;
    schema allows multiple for future multi-folder support.

    Drive folder = where the operator drops the final-cut mp4 right before
    posting to Instagram. The watcher polls this folder, transcribes new
    files, and matches them against awaiting_finishing jobs (advancing
    matched jobs to `published` with published_via='drive_watch'). Later
    when the IG sync flow processes the actual reel, it back-fills the
    instagram_url / instagram_video_id on the already-published job.
    """
    __tablename__ = "drive_accounts"

    id                       = Column(Integer, primary_key=True, autoincrement=True)
    user_id                  = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    # Google identity for display + token-scope diagnostics.
    google_email             = Column(String(255), nullable=True)
    refresh_token_encrypted  = Column(Text, nullable=False)
    # Folder being watched. ID is stable across renames; name kept for UI.
    folder_id                = Column(String(100), nullable=True)
    folder_name              = Column(String(255), nullable=True)
    last_synced_at           = Column(DateTime, nullable=True)
    added_at                 = Column(DateTime, default=datetime.utcnow)

    user   = relationship("User", back_populates="drive_accounts")
    videos = relationship(
        "DriveVideo", back_populates="account", cascade="all, delete-orphan"
    )

    def to_dict(self):
        return {
            "id": self.id,
            "google_email": self.google_email,
            "folder_id": self.folder_id,
            "folder_name": self.folder_name,
            "last_synced_at": self.last_synced_at.isoformat() if self.last_synced_at else None,
            "added_at": self.added_at.isoformat() if self.added_at else None,
        }


class DriveVideo(Base):
    """A video file pulled from a watched Drive folder."""
    __tablename__ = "drive_videos"

    id                   = Column(Integer, primary_key=True, autoincrement=True)
    account_id           = Column(Integer, ForeignKey("drive_accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    # Drive's stable file ID. Unique per file even if renamed.
    drive_file_id        = Column(String(100), nullable=False)
    name                 = Column(String(500), nullable=False)
    mime_type            = Column(String(100), nullable=True)
    size_bytes           = Column(Integer, nullable=True)
    # Drive's modifiedTime — proxy for "when did operator drop this".
    posted_at            = Column(DateTime, nullable=True, index=True)
    transcription        = Column(Text, nullable=True)
    transcription_status = Column(String(16), default="pending")
    transcription_error  = Column(Text, nullable=True)
    match_score          = Column(Float, nullable=True)
    matched_job_id       = Column(String(36), ForeignKey("jobs.id"), nullable=True, index=True)
    matched_at           = Column(DateTime, nullable=True)
    created_at           = Column(DateTime, default=datetime.utcnow)

    account     = relationship("DriveAccount", back_populates="videos")
    matched_job = relationship("Job", foreign_keys=[matched_job_id])

    __table_args__ = (
        Index("ix_drive_videos_account_file", "account_id", "drive_file_id", unique=True),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "drive_file_id": self.drive_file_id,
            "name": self.name,
            "mime_type": self.mime_type,
            "size_bytes": self.size_bytes,
            "posted_at": self.posted_at.isoformat() if self.posted_at else None,
            "transcription_status": self.transcription_status,
            "transcription_error": self.transcription_error,
            "match_score": self.match_score,
            "matched_job_id": self.matched_job_id,
        }


class LocalVideo(Base):
    """A video file uploaded from a user's local watched folder.

    Browser-based watcher uses the File System Access API:
    showDirectoryPicker() → polls directory → for each new file, POSTs
    multipart upload to /api/local-videos/upload. We dedup by file_hash
    (SHA-256 of the file bytes) so re-uploading the same file is idempotent.

    On match, advances the matched Job to lifecycle_stage='published' with
    published_via='local_watch' (parallel to drive_watch / ig_match paths).
    """
    __tablename__ = "local_videos"

    id                   = Column(Integer, primary_key=True, autoincrement=True)
    user_id              = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    # SHA-256 of file bytes, hex. 64-char unique-per-user idempotency key.
    file_hash            = Column(String(64), nullable=False)
    file_name            = Column(String(500), nullable=False)
    size_bytes           = Column(Integer, nullable=True)
    transcription        = Column(Text, nullable=True)
    transcription_status = Column(String(16), default="pending")
    transcription_error  = Column(Text, nullable=True)
    match_score          = Column(Float, nullable=True)
    matched_job_id       = Column(String(36), ForeignKey("jobs.id"), nullable=True, index=True)
    matched_at           = Column(DateTime, nullable=True)
    created_at           = Column(DateTime, default=datetime.utcnow)

    matched_job = relationship("Job", foreign_keys=[matched_job_id])

    __table_args__ = (
        Index("ix_local_videos_user_hash", "user_id", "file_hash", unique=True),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "file_hash": self.file_hash,
            "file_name": self.file_name,
            "size_bytes": self.size_bytes,
            "transcription_status": self.transcription_status,
            "transcription_error": self.transcription_error,
            "match_score": self.match_score,
            "matched_job_id": self.matched_job_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


# Database setup
engine = None
SessionLocal = None


def init_db(database_url: str = None):
    """Initialize database connection"""
    global engine, SessionLocal
    import os
    
    # Check for DATABASE_URL environment variable (for PostgreSQL on Render/Heroku)
    if database_url is None:
        database_url = os.environ.get("DATABASE_URL")
        
        # Render/Heroku uses postgres:// but SQLAlchemy needs postgresql://
        if database_url and database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    # Default to SQLite if no DATABASE_URL
    if database_url is None:
        database_url = f"sqlite:///{app_config.data_dir / 'jobs.db'}"
    
    # For SQLite with multiple workers/processes, use NullPool
    is_sqlite = "sqlite" in database_url
    is_postgres = "postgresql" in database_url
    
    print(f"[Database] Using: {'PostgreSQL' if is_postgres else 'SQLite' if is_sqlite else 'Other'}", flush=True)
    
    engine_kwargs = {
        "connect_args": {"check_same_thread": False} if is_sqlite else {},
    }
    
    if is_sqlite:
        engine_kwargs["poolclass"] = NullPool
    elif is_postgres:
        # PostgreSQL connection pooling.
        # v473b: raised from 20/40 (=60) to 30/60 (=90). Under active batch
        # processing, variant-upload handlers hold a DB connection for the
        # entire R2 upload sequence (5-30s per request). With multiple
        # workers uploading in parallel the old 60-connection ceiling got
        # exhausted, queuing subsequent requests for up to pool_timeout
        # seconds before they errored. Logs showed "QueuePool limit of
        # size 20 overflow 40 reached, connection timed out, timeout 30.00"
        # which matched the worker's 30s poll read-timeouts exactly (one
        # is the cause of the other).
        # Render's managed PostgreSQL allows ~97 connections on most
        # plans, so 90 leaves a small buffer for internal/maintenance
        # connections.
        engine_kwargs["pool_size"] = 30
        engine_kwargs["max_overflow"] = 60
        engine_kwargs["pool_pre_ping"] = True  # Check connection health before use
        # v503: pool_recycle reduced 300 → 180 seconds. Render's managed
        # Postgres idle timeout is around 5-10 minutes; recycling at 3 min
        # keeps every connection well under that window. Combined with
        # TCP keepalives below, this should eliminate the "SSL SYSCALL
        # error: EOF detected" errors that fire when SQLAlchemy tries to
        # rollback on a connection killed silently by network NAT/firewall.
        engine_kwargs["pool_recycle"] = 180
        # v473b: dropped from 30s to 5s. If a connection isn't available
        # within 5 seconds something is badly wrong — stacking requests
        # for 30s makes the pipe even more congested. Fail fast → client
        # retries → poll continues rather than blocking 30s.
        engine_kwargs["pool_timeout"] = 5
        # v503: TCP keepalives. Without these, an idle connection can be
        # silently killed by intermediate NAT/firewall (Render → managed
        # Postgres goes through their internal network). The first sign
        # is when SQLAlchemy tries to use the connection: SSL SYSCALL
        # error. With keepalives every 30s the OS detects dead
        # connections within ~90s instead of "whenever the next query
        # happens to land on this connection."
        engine_kwargs["connect_args"] = {
            "keepalives": 1,           # enable TCP keepalive
            "keepalives_idle": 30,     # send keepalive after 30s idle
            "keepalives_interval": 10, # then every 10s
            "keepalives_count": 5,     # mark dead after 5 failed probes (~90s total)
            "connect_timeout": 10,     # cap initial handshake at 10s
        }
        # Default is "rollback" but explicit is clearer.
        engine_kwargs["pool_reset_on_return"] = "rollback"
    
    engine = create_engine(database_url, **engine_kwargs)

    # ────────────────────────────────────────────────────────────────
    # v523.1: do_connect retry for Postgres post-restart WAL recovery
    # ────────────────────────────────────────────────────────────────
    # Render's managed Postgres occasionally restarts (failover, plan
    # change, maintenance). The host stays reachable and the TCP listener
    # accepts connections, but the PG process is replaying WAL to reach
    # consistent state. During this window (typically 10–30s) it rejects
    # every connection attempt with:
    #
    #   FATAL: the database system is not yet accepting connections
    #   DETAIL: Consistent recovery state has not been yet reached.
    #
    # Why existing safeguards miss this:
    #   • pool_pre_ping only tests EXISTING pool connections. After a
    #     restart there are no live connections to ping; checkout
    #     immediately tries to create a new one, which fails too.
    #   • pool_recycle and TCP keepalives address idle-connection rot,
    #     not connection-creation rejection.
    #   • The SSL SYSCALL EOF graceful-close path handles a connection
    #     dying mid-request, not a fresh connection being refused.
    #
    # This listener wraps every connection-creation attempt in a bounded
    # retry loop, gated by an allow-list of the exact PG recovery
    # messages. Auth failures, DNS errors, "too many connections", and
    # other real problems fall through and fail fast.
    #
    # The listener replaces SQLAlchemy's default connect path: we call
    # dialect.connect() ourselves and return the connection. SQLAlchemy
    # uses what we return (or what we raise).
    if is_postgres:
        _log = logging.getLogger("veo.db.recovery_retry")
        # Allow-list: substrings of the recovery messages we retry on.
        # All other OperationalErrors fall through unchanged.
        _RECOVERY_MARKERS = (
            "the database system is not yet accepting connections",
            "the database system is starting up",
            "Consistent recovery state has not been yet reached",
            "the database system is shutting down",
            "the database system is in recovery mode",
        )
        # Backoff schedule: total ~31s budget. PG recovery typically
        # finishes in 10-30s on Render, so this covers the normal case
        # without holding a request too long. Each attempt is independent
        # — if recovery takes longer than 31s, the request fails and the
        # client retries, while subsequent requests get fresh attempts.
        _RETRY_DELAYS = [1.0, 2.0, 4.0, 8.0, 16.0]  # 5 retries after initial

        def _is_recovery_error(exc) -> bool:
            msg = str(exc) or ""
            return any(m in msg for m in _RECOVERY_MARKERS)

        @event.listens_for(engine, "do_connect")
        def _retry_on_pg_recovery(dialect, conn_rec, cargs, cparams):
            """Retry connection creation on Postgres WAL-recovery rejections.

            Returns a DBAPI connection (which SQLAlchemy uses directly).
            Raises if all retries exhaust or if a non-recovery error
            occurs.
            """
            last_exc = None
            for attempt_idx, delay in enumerate([0.0] + _RETRY_DELAYS):
                if delay > 0:
                    _log.warning(
                        "[db.recovery_retry] PG in recovery, sleeping %.1fs "
                        "before retry %d/%d",
                        delay, attempt_idx, len(_RETRY_DELAYS)
                    )
                    _time.sleep(delay)
                try:
                    return dialect.connect(*cargs, **cparams)
                except Exception as exc:
                    if _is_recovery_error(exc):
                        last_exc = exc
                        continue
                    # Real error: don't retry, propagate immediately.
                    raise
            # Exhausted: re-raise the last recovery error so the caller
            # gets a 500 and Render's request will be retried by the
            # client. The error message will clearly identify the cause.
            _log.error(
                "[db.recovery_retry] PG still in recovery after %d retries "
                "(~%.0fs total budget) — giving up",
                len(_RETRY_DELAYS), sum(_RETRY_DELAYS)
            )
            if last_exc is not None:
                raise last_exc
            # Defensive: should be unreachable
            raise RuntimeError("do_connect retry loop exited without exception or success")

        print("[Database] ✓ Registered do_connect retry listener for PG recovery state", flush=True)

        # ────────────────────────────────────────────────────────────────
        # v543: handle_error for dead-connection auto-invalidation
        # ────────────────────────────────────────────────────────────────
        # When a query fails with a dead-connection error (SSL SYSCALL EOF,
        # "server closed the connection unexpectedly", etc.) SQLAlchemy
        # by default keeps the connection in the pool — meaning the *next*
        # request that checks it out also gets a dead one. pool_pre_ping
        # catches *idle* deaths but NOT mid-query deaths (the connection
        # was alive when checked out, then died during the SELECT).
        #
        # The handle_error event fires for every DBAPI error and lets us
        # set ``ctx.is_disconnect = True``. SQLAlchemy then automatically
        # invalidates the connection and removes it from the pool. The
        # next checkout creates a fresh one. This is the documented fix
        # for "SSL SYSCALL error: EOF detected" propagating into multiple
        # subsequent requests.
        @event.listens_for(engine, "handle_error")
        def _mark_dead_connection_on_error(ctx):
            exc = ctx.original_exception
            if exc is None:
                return
            err_str = str(exc).lower()
            is_dead_conn = (
                'ssl syscall error' in err_str
                or 'eof detected' in err_str
                or 'server closed the connection' in err_str
                or 'connection has been closed' in err_str
                or 'connection is closed' in err_str
                or 'terminating connection due to' in err_str
                or 'broken pipe' in err_str
            )
            if is_dead_conn:
                # Tell SQLAlchemy to treat this as a disconnect: the
                # connection is invalidated and removed from the pool.
                # Without this flag, the dead connection sits in the
                # pool waiting to be handed to the next request.
                ctx.is_disconnect = True

        print("[Database] ✓ Registered handle_error listener for dead-connection auto-invalidation", flush=True)
        # ────────────────────────────────────────────────────────────────

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    # Run migrations for new columns
    if is_sqlite:
        _run_migrations_sqlite(engine)
    else:
        _run_migrations_postgresql(engine)
    
    return engine


def _run_migrations_postgresql(engine):
    """Add new columns to existing tables if they don't exist (PostgreSQL)"""
    from sqlalchemy import text
    
    migrations = [
        # (table, column, sql)
        ("clips", "selected_variant", "ALTER TABLE clips ADD COLUMN IF NOT EXISTS selected_variant INTEGER DEFAULT 1"),
        ("clips", "kling_variant_status", "ALTER TABLE clips ADD COLUMN IF NOT EXISTS kling_variant_status VARCHAR(20)"),
        ("jobs", "user_id", "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS user_id TEXT"),
        ("user_api_keys", "key_status", "ALTER TABLE user_api_keys ADD COLUMN IF NOT EXISTS key_status TEXT DEFAULT 'unknown'"),
        ("user_api_keys", "last_checked", "ALTER TABLE user_api_keys ADD COLUMN IF NOT EXISTS last_checked TIMESTAMP"),
        # Flow backend fields
        ("jobs", "backend", "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS backend TEXT DEFAULT 'api'"),
        ("jobs", "flow_project_url", "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS flow_project_url TEXT"),
        ("jobs", "flow_state_json", "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS flow_state_json TEXT"),
        ("jobs", "flow_needs_auth", "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS flow_needs_auth BOOLEAN DEFAULT FALSE"),
        ("clips", "flow_clip_id", "ALTER TABLE clips ADD COLUMN IF NOT EXISTS flow_clip_id TEXT"),
        ("clips", "output_url", "ALTER TABLE clips ADD COLUMN IF NOT EXISTS output_url TEXT"),
        # Storyboard/Scene mode fields
        ("clips", "clip_mode", "ALTER TABLE clips ADD COLUMN IF NOT EXISTS clip_mode TEXT DEFAULT 'fresh'"),  # v782 default fresh
        ("clips", "scene_index", "ALTER TABLE clips ADD COLUMN IF NOT EXISTS scene_index INTEGER DEFAULT 0"),
        # v805 — Prompt B policy fallback (voice-only), shipped verbatim to the worker
        ("clips", "prompt_text_b", "ALTER TABLE clips ADD COLUMN IF NOT EXISTS prompt_text_b TEXT"),
        # User Worker Token table
        ("user_worker_tokens", "_create_table_", """
            CREATE TABLE IF NOT EXISTS user_worker_tokens (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id),
                name TEXT DEFAULT 'My Worker',
                created_at TIMESTAMP DEFAULT NOW(),
                last_seen TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """),
        # Export / Voice Clone tracking
        ("jobs", "has_export", "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS has_export BOOLEAN DEFAULT FALSE"),
        ("jobs", "has_voice_clone", "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS has_voice_clone BOOLEAN DEFAULT FALSE"),
        # v455: worker abort signal
        ("jobs", "abort_requested", "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS abort_requested BOOLEAN DEFAULT FALSE"),
        # v759: per-user image worker scoping
        ("image_worker_heartbeats", "user_id", "ALTER TABLE image_worker_heartbeats ADD COLUMN IF NOT EXISTS user_id TEXT"),
        # Post-render lifecycle tracker (2026-05-29)
        ("jobs", "lifecycle_stage", "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lifecycle_stage VARCHAR(32)"),
        ("jobs", "approval_at",     "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS approval_at TIMESTAMP"),
        ("jobs", "export_at",       "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS export_at TIMESTAMP"),
        ("jobs", "finishing_at",    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS finishing_at TIMESTAMP"),
        ("jobs", "published_at",    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS published_at TIMESTAMP"),
        ("jobs", "notes",           "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS notes TEXT"),
        ("jobs", "archived",        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS archived BOOLEAN DEFAULT FALSE"),
        ("jobs", "instagram_url",      "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS instagram_url VARCHAR(500)"),
        ("jobs", "instagram_video_id", "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS instagram_video_id INTEGER"),
        # 2026-06-01: drive-watch lifecycle path
        ("jobs", "published_via",      "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS published_via VARCHAR(20)"),
        # v815 — per-account settings JSON blob (auto_image_retry mode, etc.)
        ("users", "settings_json", "ALTER TABLE users ADD COLUMN IF NOT EXISTS settings_json TEXT"),
        # v815 — auto-image-retry audit trail per clip
        ("clips", "auto_image_retry_json",
         "ALTER TABLE clips ADD COLUMN IF NOT EXISTS auto_image_retry_json TEXT"),
    ]

    with engine.connect() as conn:
        for table, column, sql in migrations:
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"[Migration] PostgreSQL: ensured column {column} exists in {table}", flush=True)
            except Exception as e:
                print(f"[Migration] PostgreSQL skipped {column}: {e}", flush=True)

    # v726: indexes for since_days date-window filter + v727 status diff endpoint.
    # Compound (user_id, created_at DESC) lets the ORDER BY + LIMIT path
    # walk inside the user partition. Compound (job_id, status) supports
    # the v727 /clips/active endpoint filtering by status per job.
    # CREATE INDEX IF NOT EXISTS is idempotent in Postgres 9.5+.
    index_migrations = [
        "CREATE INDEX IF NOT EXISTS ix_jobs_user_created ON jobs (user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_clips_job_status ON clips (job_id, status)",
        "CREATE INDEX IF NOT EXISTS ix_jobs_user_lifecycle ON jobs (user_id, lifecycle_stage, archived)",
        "CREATE INDEX IF NOT EXISTS ix_jobs_instagram_video_id ON jobs (instagram_video_id)",
    ]
    with engine.connect() as conn:
        for sql in index_migrations:
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"[Migration] PostgreSQL: ensured index — {sql}", flush=True)
            except Exception as e:
                print(f"[Migration] PostgreSQL skipped index: {e}", flush=True)

    # 2026-05-31: Instagram CDN thumb URLs are ~800 chars; widen the column.
    # Safe to run repeatedly — no-op if already TEXT.
    alter_migrations = [
        "ALTER TABLE instagram_videos ALTER COLUMN thumb_url TYPE TEXT",
        "ALTER TABLE instagram_videos ADD COLUMN IF NOT EXISTS video_url TEXT",
        "ALTER TABLE instagram_videos ADD COLUMN IF NOT EXISTS thumb_r2_key TEXT",
    ]
    with engine.connect() as conn:
        for sql in alter_migrations:
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"[Migration] PostgreSQL: ALTER ok — {sql}", flush=True)
            except Exception as e:
                print(f"[Migration] PostgreSQL skipped ALTER: {e}", flush=True)

    # v776: one-shot backfill — move already-completed jobs into the tracker.
    # Idempotent via the `lifecycle_stage IS NULL` guard.
    # has_export=True implies all-clips-approved + export-final ran, so those
    # jobs skip approval+export and land in awaiting_finishing (first manual
    # stage: captions/touches in CapCut).
    backfill_sql = """
        UPDATE jobs
        SET lifecycle_stage = 'awaiting_finishing',
            approval_at     = COALESCE(completed_at, NOW()),
            export_at       = COALESCE(completed_at, NOW()),
            finishing_at    = COALESCE(completed_at, NOW())
        WHERE status = 'completed'
          AND has_export = TRUE
          AND lifecycle_stage IS NULL
    """
    with engine.connect() as conn:
        try:
            result = conn.execute(text(backfill_sql))
            conn.commit()
            print(f"[Migration] PostgreSQL: lifecycle backfill moved {result.rowcount} jobs to awaiting_finishing", flush=True)
        except Exception as e:
            print(f"[Migration] PostgreSQL skipped lifecycle backfill: {e}", flush=True)

    # v776.1: one-shot reclassify — jobs sitting in awaiting_approval with
    # has_export=True were misclassified by the original backfill. Advance
    # them to awaiting_finishing. Idempotent (second run finds 0 matching rows).
    reclassify_sql = """
        UPDATE jobs
        SET lifecycle_stage = 'awaiting_finishing',
            export_at       = COALESCE(export_at, approval_at, completed_at, NOW()),
            finishing_at    = COALESCE(finishing_at, approval_at, completed_at, NOW())
        WHERE status = 'completed'
          AND has_export = TRUE
          AND lifecycle_stage = 'awaiting_approval'
    """
    with engine.connect() as conn:
        try:
            result = conn.execute(text(reclassify_sql))
            conn.commit()
            print(f"[Migration] PostgreSQL: v776.1 reclassify moved {result.rowcount} jobs to awaiting_finishing", flush=True)
        except Exception as e:
            print(f"[Migration] PostgreSQL skipped v776.1 reclassify: {e}", flush=True)

    return engine


def _run_migrations_sqlite(engine):
    """Add new columns to existing tables if they don't exist (SQLite only)"""
    from sqlalchemy import text
    
    migrations = [
        # Add selected_variant column to clips table
        ("clips", "selected_variant", "ALTER TABLE clips ADD COLUMN selected_variant INTEGER DEFAULT 1"),
        # Add user_id column to jobs table
        ("jobs", "user_id", "ALTER TABLE jobs ADD COLUMN user_id TEXT"),
        # Add key_status column to user_api_keys table
        ("user_api_keys", "key_status", "ALTER TABLE user_api_keys ADD COLUMN key_status TEXT DEFAULT 'unknown'"),
        # Add last_checked column to user_api_keys table
        ("user_api_keys", "last_checked", "ALTER TABLE user_api_keys ADD COLUMN last_checked DATETIME"),
        # Flow backend fields - jobs table
        ("jobs", "backend", "ALTER TABLE jobs ADD COLUMN backend TEXT DEFAULT 'api'"),
        ("jobs", "flow_project_url", "ALTER TABLE jobs ADD COLUMN flow_project_url TEXT"),
        ("jobs", "flow_state_json", "ALTER TABLE jobs ADD COLUMN flow_state_json TEXT"),
        ("jobs", "flow_needs_auth", "ALTER TABLE jobs ADD COLUMN flow_needs_auth INTEGER DEFAULT 0"),
        # Flow backend fields - clips table
        ("clips", "flow_clip_id", "ALTER TABLE clips ADD COLUMN flow_clip_id TEXT"),
        ("clips", "output_url", "ALTER TABLE clips ADD COLUMN output_url TEXT"),
        # Storyboard/Scene mode fields
        ("clips", "clip_mode", "ALTER TABLE clips ADD COLUMN clip_mode TEXT DEFAULT 'fresh'"),  # v782 default fresh
        ("clips", "scene_index", "ALTER TABLE clips ADD COLUMN scene_index INTEGER DEFAULT 0"),
        # Export / Voice Clone tracking
        ("jobs", "has_export", "ALTER TABLE jobs ADD COLUMN has_export INTEGER DEFAULT 0"),
        ("jobs", "has_voice_clone", "ALTER TABLE jobs ADD COLUMN has_voice_clone INTEGER DEFAULT 0"),
        # v455: worker abort signal
        ("jobs", "abort_requested", "ALTER TABLE jobs ADD COLUMN abort_requested INTEGER DEFAULT 0"),
        # v759: per-user image worker scoping
        ("image_worker_heartbeats", "user_id", "ALTER TABLE image_worker_heartbeats ADD COLUMN user_id TEXT"),
        # Post-render lifecycle tracker (2026-05-29)
        ("jobs", "lifecycle_stage", "ALTER TABLE jobs ADD COLUMN lifecycle_stage TEXT"),
        ("jobs", "approval_at",     "ALTER TABLE jobs ADD COLUMN approval_at DATETIME"),
        ("jobs", "export_at",       "ALTER TABLE jobs ADD COLUMN export_at DATETIME"),
        ("jobs", "finishing_at",    "ALTER TABLE jobs ADD COLUMN finishing_at DATETIME"),
        ("jobs", "published_at",    "ALTER TABLE jobs ADD COLUMN published_at DATETIME"),
        ("jobs", "notes",           "ALTER TABLE jobs ADD COLUMN notes TEXT"),
        ("jobs", "archived",        "ALTER TABLE jobs ADD COLUMN archived INTEGER DEFAULT 0"),
        ("jobs", "instagram_url",      "ALTER TABLE jobs ADD COLUMN instagram_url TEXT"),
        ("jobs", "instagram_video_id", "ALTER TABLE jobs ADD COLUMN instagram_video_id INTEGER"),
        ("instagram_videos", "video_url", "ALTER TABLE instagram_videos ADD COLUMN video_url TEXT"),
        # 2026-06-01: drive-watch lifecycle path
        ("jobs", "published_via",      "ALTER TABLE jobs ADD COLUMN published_via TEXT"),
        # v815 — per-account settings JSON blob (auto_image_retry mode, etc.)
        ("users", "settings_json", "ALTER TABLE users ADD COLUMN settings_json TEXT"),
        # v815 — auto-image-retry audit trail per clip
        ("clips", "auto_image_retry_json",
         "ALTER TABLE clips ADD COLUMN auto_image_retry_json TEXT"),
    ]

    with engine.connect() as conn:
        for table, column, sql in migrations:
            try:
                # Check if column exists (SQLite PRAGMA)
                result = conn.execute(text(f"PRAGMA table_info({table})"))
                columns = [row[1] for row in result]
                
                if column not in columns:
                    conn.execute(text(sql))
                    conn.commit()
                    print(f"[Migration] Added column {column} to {table}", flush=True)
            except Exception as e:
                print(f"[Migration] Skipped {column}: {e}", flush=True)
    
    # Create user_worker_tokens table if not exists (SQLite)
    with engine.connect() as conn:
        try:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_worker_tokens (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(id),
                    name TEXT DEFAULT 'My Worker',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP,
                    is_active INTEGER DEFAULT 1
                )
            """))
            conn.commit()
            print("[Migration] Ensured user_worker_tokens table exists", flush=True)
        except Exception as e:
            print(f"[Migration] user_worker_tokens table: {e}", flush=True)

    # v726: SQLite indexes — same compound shapes as the Postgres path.
    # CREATE INDEX IF NOT EXISTS is supported in SQLite 3.3+.
    sqlite_indexes = [
        "CREATE INDEX IF NOT EXISTS ix_jobs_user_created ON jobs (user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_clips_job_status ON clips (job_id, status)",
    ]
    with engine.connect() as conn:
        for sql in sqlite_indexes:
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"[Migration] SQLite: ensured index — {sql}", flush=True)
            except Exception as e:
                print(f"[Migration] SQLite skipped index: {e}", flush=True)

    # v776: lifecycle index + one-shot backfill (SQLite).
    with engine.connect() as conn:
        try:
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_jobs_user_lifecycle "
                "ON jobs (user_id, lifecycle_stage, archived)"
            ))
            conn.commit()
        except Exception as e:
            print(f"[Migration] SQLite skipped lifecycle index: {e}", flush=True)
        try:
            result = conn.execute(text(
                "UPDATE jobs "
                "SET lifecycle_stage = 'awaiting_approval', "
                "    approval_at     = COALESCE(completed_at, CURRENT_TIMESTAMP) "
                "WHERE status = 'completed' "
                "  AND has_export = 1 "
                "  AND lifecycle_stage IS NULL"
            ))
            conn.commit()
            print(f"[Migration] SQLite: lifecycle backfill moved {result.rowcount} jobs to awaiting_approval", flush=True)
        except Exception as e:
            print(f"[Migration] SQLite skipped lifecycle backfill: {e}", flush=True)


@contextmanager
def get_db() -> Session:
    """Get database session as context manager.

    v543 — graceful EOF handling. The bare get_db() context manager
    used to call ``db.close()`` directly on exit. When the underlying
    connection had been silently killed by network NAT/firewall mid-
    request, the close() call would raise ``OperationalError: SSL
    SYSCALL error: EOF detected``. The exception then propagated up
    through whatever middleware called us — polluting logs with a
    multi-page stack trace even though the HTTP response had already
    been delivered. Mirror the same defensive close logic from
    get_db_session() so the middleware path is just as resilient.
    """
    if SessionLocal is None:
        init_db()
    
    db = SessionLocal()
    try:
        yield db
    finally:
        try:
            db.close()
        except Exception as e:
            err_str = str(e).lower()
            is_ssl_eof = (
                'ssl syscall error' in err_str
                or 'eof detected' in err_str
                or 'connection has been closed' in err_str
                or 'server closed the connection' in err_str
            )
            if is_ssl_eof:
                # Dead connection. Force-invalidate so SQLAlchemy
                # removes it from the pool — a fresh one is created
                # on next checkout. Don't re-raise; the request has
                # already returned to the client.
                try:
                    db.invalidate()
                except Exception:
                    pass
            else:
                import logging as _lg
                _lg.warning(f"[DB] Unexpected close error in get_db(): {e}")
                try:
                    db.invalidate()
                except Exception:
                    pass


def get_db_session() -> Session:
    """Get database session (for FastAPI dependency injection)"""
    if SessionLocal is None:
        init_db()
    
    db = SessionLocal()
    try:
        yield db
    finally:
        # v503: graceful close. If the connection was killed mid-request
        # (Render network blip, Postgres idle timeout, slow query exceeding
        # network keepalive), db.close() runs an automatic rollback which
        # then raises `psycopg2.OperationalError: SSL SYSCALL error: EOF
        # detected`. The HTTP response has usually already been sent — this
        # error only pollutes logs and risks the dead connection being
        # returned to the pool.
        #
        # Strategy: try normal close. If that fails (connection dead),
        # invalidate the underlying connection so SQLAlchemy discards it
        # from the pool instead of reusing it.
        try:
            db.close()
        except Exception as e:
            # Identify the SSL/EOF case specifically — log at INFO since
            # recovery is automatic. Other exceptions should still be
            # visible.
            err_str = str(e).lower()
            is_ssl_eof = (
                'ssl syscall error' in err_str
                or 'eof detected' in err_str
                or 'connection has been closed' in err_str
                or 'server closed the connection' in err_str
            )
            if is_ssl_eof:
                # Dead connection. Force-invalidate so it's removed from
                # the pool. SQLAlchemy will create a fresh one next time.
                try:
                    db.invalidate()
                except Exception:
                    pass
                # Don't re-raise — the request already finished.
            else:
                # Other close errors are unexpected; log but don't crash
                # the request handler.
                import logging as _lg
                _lg.warning(f"[DB] Unexpected close error: {e}")
                try:
                    db.invalidate()
                except Exception:
                    pass


def read_query_with_retry(db: Session, query_fn, max_attempts: int = 2):
    """v543 — retry a read-only query once on dead-connection errors.

    Despite ``pool_pre_ping=True`` and ``pool_recycle=180s``, a Render
    Postgres connection can die mid-query (managed PG restart, network
    blip, or NAT idle timeout firing exactly at query time). pre_ping
    can't help here — by the time the SELECT is in flight, the ping
    is already in the past. The clean fix used by every production
    SQLAlchemy app is to retry idempotent reads once.

    Usage::

        results = read_query_with_retry(db, lambda: db.query(Foo).all())

    SQLAlchemy doesn't auto-retry because retries can break writes
    (an INSERT that "fails" with EOF may have actually succeeded
    server-side). For SELECTs there's no such risk — they're pure.

    On the second failure we re-raise. If both attempts fail, the
    database is genuinely down and FastAPI's exception handler should
    return a 500 to the client.
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return query_fn()
        except SQLAOperationalError as e:
            last_exc = e
            err_str = str(e).lower()
            is_dead_conn = (
                'ssl syscall error' in err_str
                or 'eof detected' in err_str
                or 'connection has been closed' in err_str
                or 'server closed the connection' in err_str
                or 'connection is closed' in err_str
            )
            if not is_dead_conn or attempt >= max_attempts:
                raise
            # Dead-connection error on attempt N — invalidate the
            # underlying connection, force a rollback to clear
            # transaction state, then retry. SQLAlchemy will check
            # out a fresh connection from the pool on the next query.
            import logging as _lg
            _lg.info(f"[DB] Read-query retry {attempt}/{max_attempts} after dead-connection error")
            try:
                db.rollback()
            except Exception:
                pass
            try:
                db.close()
            except Exception:
                pass
            # Tiny back-off so a thundering-herd of dead connections
            # doesn't all hit the new pool entry simultaneously.
            _time.sleep(0.1 * attempt)
    # Defensive — should not reach here.
    if last_exc:
        raise last_exc
    raise RuntimeError("read_query_with_retry exhausted attempts without error")


# Helper functions

def add_job_log(
    db: Session,
    job_id: str,
    message: str,
    level: str = "INFO",
    category: str = None,
    clip_index: int = None,
    details: Dict = None
):
    """Add a log entry for a job"""
    log = JobLog(
        job_id=job_id,
        level=level,
        category=category,
        clip_index=clip_index,
        message=message,
        details_json=json.dumps(details) if details else None
    )
    db.add(log)
    db.commit()
    return log


def get_job_logs_since(db: Session, job_id: str, since_id: int = 0) -> List[JobLog]:
    """Get logs for a job since a given ID (for polling)"""
    return db.query(JobLog).filter(
        JobLog.job_id == job_id,
        JobLog.id > since_id
    ).order_by(JobLog.id.asc()).all()


def update_job_progress(db: Session, job_id: str):
    """Recalculate job progress from clips"""
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        return
    
    clips = db.query(Clip).filter(Clip.job_id == job_id).all()
    
    total = len(clips)
    completed = sum(1 for c in clips if c.status == ClipStatus.COMPLETED.value)
    failed = sum(1 for c in clips if c.status == ClipStatus.FAILED.value)
    skipped = sum(1 for c in clips if c.status == ClipStatus.SKIPPED.value)
    
    job.total_clips = total
    job.completed_clips = completed
    job.failed_clips = failed
    job.skipped_clips = skipped
    
    if total > 0:
        job.progress_percent = ((completed + skipped) / total) * 100
    
    db.commit()