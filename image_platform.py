# -*- coding: utf-8 -*-
"""
Image Platform — node-graph image generation via Flow UI worker.

A standalone module plugged into the main FastAPI app. Provides:
  • SQLAlchemy models for nodes, variants, and parent→child edges
  • REST endpoints at /api/images/...
  • A watch-folder bridge to image_worker.py (drops job JSONs,
    polls .done.json files, updates the DB)

Design principles:
  • Uploaded reference images are stored as "seed nodes" (status=ready,
    one variant = the uploaded file). Keeps the graph uniform.
  • A node can have up to 3 parent nodes (subject / background / ref).
    Parent = "use the chosen variant of parent N as reference image N".
  • Regenerating a node trashes the old variants (files + DB rows) and
    kicks off a fresh generation. Chosen variant is reset.
"""

from __future__ import annotations

import os
import json
import uuid
import shutil
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Set, Tuple

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Header, Request, Body, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import (
    Column, Integer, String, Text, DateTime, ForeignKey, Boolean, Float
)
from sqlalchemy.orm import Session, relationship, joinedload, selectinload

# Reuse the existing SQLAlchemy Base so init_db() picks up these models
from models import Base, get_db_session, get_db, User, read_query_with_retry
from config import app_config
from auth import get_current_user


log = logging.getLogger("image_platform")


# =============================================================================
# Migrations (run once at startup, after init_db)
# =============================================================================

def run_image_platform_migrations():
    """Add columns on image_nodes that may not exist on older DBs.
    Called once from main.py lifespan, after init_db()."""
    from models import engine
    from sqlalchemy import text, inspect

    if engine is None:
        return
    is_sqlite = engine.url.drivername.startswith("sqlite")

    # The full set of columns we may need to add if user is upgrading
    sqlite_migrations = [
        ("image_nodes", "claimed_by_worker",
         "ALTER TABLE image_nodes ADD COLUMN claimed_by_worker TEXT"),
        ("image_nodes", "claimed_at",
         "ALTER TABLE image_nodes ADD COLUMN claimed_at DATETIME"),
        # Scene-table import metadata (added v428 — supports "Promote to video")
        ("image_nodes", "batch_id",
         "ALTER TABLE image_nodes ADD COLUMN batch_id TEXT"),
        ("image_nodes", "scene_index_in_batch",
         "ALTER TABLE image_nodes ADD COLUMN scene_index_in_batch INTEGER"),
        ("image_nodes", "voiceover_text",
         "ALTER TABLE image_nodes ADD COLUMN voiceover_text TEXT"),
        ("image_nodes", "scene_transition",
         "ALTER TABLE image_nodes ADD COLUMN scene_transition TEXT"),
        ("image_nodes", "clip_mode",
         "ALTER TABLE image_nodes ADD COLUMN clip_mode TEXT"),
        ("image_nodes", "visual_register",
         "ALTER TABLE image_nodes ADD COLUMN visual_register TEXT"),
        ("image_nodes", "rhythm_tier",
         "ALTER TABLE image_nodes ADD COLUMN rhythm_tier TEXT"),
        ("image_nodes", "action_note",
         "ALTER TABLE image_nodes ADD COLUMN action_note TEXT"),
        # v429: video-mode hints on the batch row
        ("image_job_batches", "video_mode",
         "ALTER TABLE image_job_batches ADD COLUMN video_mode TEXT"),
        ("image_job_batches", "auto_split",
         "ALTER TABLE image_job_batches ADD COLUMN auto_split INTEGER DEFAULT 0"),
        # v447: user scoping — every node + batch owned by a user.
        # NULL on existing rows until _backfill_user_id_ownership runs.
        ("image_nodes", "user_id",
         "ALTER TABLE image_nodes ADD COLUMN user_id TEXT"),
        ("image_job_batches", "user_id",
         "ALTER TABLE image_job_batches ADD COLUMN user_id TEXT"),
        # v530: source discriminator on image_variants — 'ai' | 'manual'.
        # Existing rows are all AI-generated; default 'ai' backfills them.
        ("image_variants", "source",
         "ALTER TABLE image_variants ADD COLUMN source TEXT DEFAULT 'ai' NOT NULL"),
        # v537: explicit per-scene speaker mode declared in the markdown.
        # NULL = auto-detect (preserves the existing _detect_voiceover_only
        # behavior). Non-NULL values: 'on-camera' | 'voiceover' | 'auto'.
        # 'auto' is identical to NULL but lets the writer make the intent
        # explicit when reading the script. Backfilled NULL on existing rows.
        ("image_nodes", "speaker_mode",
         "ALTER TABLE image_nodes ADD COLUMN speaker_mode TEXT"),
        # v572: explicit per-clip Veo prompt overrides from markdown.
        # NULL = use build_prompt's auto-construction (pre-v572 default).
        # Non-NULL = ship the prebuilt prompt to Veo verbatim, bypassing
        # build_prompt entirely. These two columns are the denormalized
        # FIRST clip's overrides for the UI thumbnail card; authoritative
        # multi-line storage is in ImageSceneAssignment.veo_prompts_json.
        ("image_nodes", "veo_prompt_override",
         "ALTER TABLE image_nodes ADD COLUMN veo_prompt_override TEXT"),
        ("image_nodes", "veo_negative_prompt_override",
         "ALTER TABLE image_nodes ADD COLUMN veo_negative_prompt_override TEXT"),
        ("image_scene_assignments", "veo_prompts_json",
         "ALTER TABLE image_scene_assignments ADD COLUMN veo_prompts_json TEXT"),
        # v573: ingredient-type discriminator on image_edges. Lets the
        # per-slot manifest builder emit the right role line per slot
        # (persona vs product vs chain). NULL on existing rows; new
        # imports populate it for upload-backed edges. Falls through to
        # role-pattern detection when NULL, so legacy edges still work.
        ("image_edges", "kind",
         "ALTER TABLE image_edges ADD COLUMN kind TEXT"),
        # v644: per-line audio-padding suffixes on the assignment row.
        # Parallel to lines_json. NULL = no pads anywhere; populated =
        # JSON array of (str | null) per line. Veo prompt builder
        # appends pad after the line; whisper-VAD ignores it.
        ("image_scene_assignments", "pads_json",
         "ALTER TABLE image_scene_assignments ADD COLUMN pads_json TEXT"),
        # v644: per-clip audio-padding suffix on the clips row (denorm
        # of the assignment's pads_json so the Veo prompt builder can
        # read it without re-joining).
        ("clips", "dialogue_pad",
         "ALTER TABLE clips ADD COLUMN dialogue_pad TEXT"),
        # v667: per-image transformation metadata (frame_anchor_s anchors
        # the image to a source-video timestamp; visual_delta describes
        # the diff vs the prior chained image; narrative_lens is an
        # optional lens label). All NULL on pre-v667 imports.
        ("image_nodes", "frame_anchor_s",
         "ALTER TABLE image_nodes ADD COLUMN frame_anchor_s REAL"),
        ("image_nodes", "visual_delta",
         "ALTER TABLE image_nodes ADD COLUMN visual_delta TEXT"),
        ("image_nodes", "narrative_lens",
         "ALTER TABLE image_nodes ADD COLUMN narrative_lens VARCHAR(40)"),
        # v668: per-scene cut mode (whisper | timeline | auto).
        ("image_scene_assignments", "cut_mode",
         "ALTER TABLE image_scene_assignments ADD COLUMN cut_mode VARCHAR(20)"),
        # v667/v668: per-clip denorm of cut_mode plus anchor-derived
        # target_duration_s and the Veo render-bucket pick.
        ("clips", "cut_mode",
         "ALTER TABLE clips ADD COLUMN cut_mode VARCHAR(20)"),
        ("clips", "target_duration_s",
         "ALTER TABLE clips ADD COLUMN target_duration_s REAL"),
        ("clips", "veo_render_duration_s",
         "ALTER TABLE clips ADD COLUMN veo_render_duration_s INTEGER"),
        # v681: multi-character cast model + text-card scene type.
        ("image_nodes", "cast_json",
         "ALTER TABLE image_nodes ADD COLUMN cast_json TEXT"),
        ("image_scene_assignments", "cast_json",
         "ALTER TABLE image_scene_assignments ADD COLUMN cast_json TEXT"),
        ("image_scene_assignments", "scene_type",
         "ALTER TABLE image_scene_assignments ADD COLUMN scene_type VARCHAR(20)"),
        ("image_scene_assignments", "caption",
         "ALTER TABLE image_scene_assignments ADD COLUMN caption TEXT"),
        ("image_scene_assignments", "bg_color",
         "ALTER TABLE image_scene_assignments ADD COLUMN bg_color VARCHAR(20)"),
        ("image_scene_assignments", "duration_s",
         "ALTER TABLE image_scene_assignments ADD COLUMN duration_s REAL"),
        ("clips", "caption",
         "ALTER TABLE clips ADD COLUMN caption TEXT"),
        ("clips", "scene_type",
         "ALTER TABLE clips ADD COLUMN scene_type VARCHAR(20)"),
        ("clips", "bg_color",
         "ALTER TABLE clips ADD COLUMN bg_color VARCHAR(20)"),
    ]
    postgres_migrations = [
        ("image_nodes", "claimed_by_worker",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS claimed_by_worker TEXT"),
        ("image_nodes", "claimed_at",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP"),
        ("image_nodes", "batch_id",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS batch_id VARCHAR(36)"),
        ("image_nodes", "scene_index_in_batch",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS scene_index_in_batch INTEGER"),
        ("image_nodes", "voiceover_text",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS voiceover_text TEXT"),
        ("image_nodes", "scene_transition",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS scene_transition VARCHAR(50)"),
        ("image_nodes", "clip_mode",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS clip_mode VARCHAR(50)"),
        ("image_nodes", "visual_register",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS visual_register VARCHAR(50)"),
        ("image_nodes", "rhythm_tier",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS rhythm_tier VARCHAR(100)"),
        ("image_nodes", "action_note",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS action_note TEXT"),
        # v537: explicit per-scene speaker mode declared in the markdown.
        ("image_nodes", "speaker_mode",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS speaker_mode VARCHAR(20)"),
        # v572: explicit per-clip Veo prompt overrides from markdown
        # (denorm of first clip on the node, full multi-line array on
        # the assignment row). See sqlite_migrations above for rationale.
        ("image_nodes", "veo_prompt_override",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS veo_prompt_override TEXT"),
        ("image_nodes", "veo_negative_prompt_override",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS veo_negative_prompt_override TEXT"),
        ("image_scene_assignments", "veo_prompts_json",
         "ALTER TABLE image_scene_assignments ADD COLUMN IF NOT EXISTS veo_prompts_json TEXT"),
        # v644: per-line audio-padding suffixes (postgres variant).
        ("image_scene_assignments", "pads_json",
         "ALTER TABLE image_scene_assignments ADD COLUMN IF NOT EXISTS pads_json TEXT"),
        ("clips", "dialogue_pad",
         "ALTER TABLE clips ADD COLUMN IF NOT EXISTS dialogue_pad TEXT"),
        # v667: per-image transformation metadata.
        ("image_nodes", "frame_anchor_s",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS frame_anchor_s DOUBLE PRECISION"),
        ("image_nodes", "visual_delta",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS visual_delta TEXT"),
        ("image_nodes", "narrative_lens",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS narrative_lens VARCHAR(40)"),
        # v668: per-scene cut mode (whisper | timeline | auto).
        ("image_scene_assignments", "cut_mode",
         "ALTER TABLE image_scene_assignments ADD COLUMN IF NOT EXISTS cut_mode VARCHAR(20)"),
        # v667/v668: per-clip cut-mode denorm + anchor-derived durations.
        ("clips", "cut_mode",
         "ALTER TABLE clips ADD COLUMN IF NOT EXISTS cut_mode VARCHAR(20)"),
        ("clips", "target_duration_s",
         "ALTER TABLE clips ADD COLUMN IF NOT EXISTS target_duration_s DOUBLE PRECISION"),
        ("clips", "veo_render_duration_s",
         "ALTER TABLE clips ADD COLUMN IF NOT EXISTS veo_render_duration_s INTEGER"),
        # v681: multi-character cast model + text-card scene type.
        ("image_nodes", "cast_json",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS cast_json TEXT"),
        ("image_scene_assignments", "cast_json",
         "ALTER TABLE image_scene_assignments ADD COLUMN IF NOT EXISTS cast_json TEXT"),
        ("image_scene_assignments", "scene_type",
         "ALTER TABLE image_scene_assignments ADD COLUMN IF NOT EXISTS scene_type VARCHAR(20)"),
        ("image_scene_assignments", "caption",
         "ALTER TABLE image_scene_assignments ADD COLUMN IF NOT EXISTS caption TEXT"),
        ("image_scene_assignments", "bg_color",
         "ALTER TABLE image_scene_assignments ADD COLUMN IF NOT EXISTS bg_color VARCHAR(20)"),
        ("image_scene_assignments", "duration_s",
         "ALTER TABLE image_scene_assignments ADD COLUMN IF NOT EXISTS duration_s DOUBLE PRECISION"),
        ("clips", "caption",
         "ALTER TABLE clips ADD COLUMN IF NOT EXISTS caption TEXT"),
        ("clips", "scene_type",
         "ALTER TABLE clips ADD COLUMN IF NOT EXISTS scene_type VARCHAR(20)"),
        ("clips", "bg_color",
         "ALTER TABLE clips ADD COLUMN IF NOT EXISTS bg_color VARCHAR(20)"),
        # v681 — text_card scenes have no image binding; drop NOT NULL on
        # image_node_id. SQLite tolerates the existing column shape (it
        # accepts NULL in INTEGER NOT NULL when re-bound at row-level —
        # SQLite's strict mode is off by default), so no SQLite ALTER.
        ("image_scene_assignments", "image_node_id_nullable",
         "ALTER TABLE image_scene_assignments ALTER COLUMN image_node_id DROP NOT NULL"),
        # v429
        ("image_job_batches", "video_mode",
         "ALTER TABLE image_job_batches ADD COLUMN IF NOT EXISTS video_mode VARCHAR(20)"),
        ("image_job_batches", "auto_split",
         "ALTER TABLE image_job_batches ADD COLUMN IF NOT EXISTS auto_split BOOLEAN DEFAULT FALSE"),
        # v447: user scoping
        ("image_nodes", "user_id",
         "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS user_id VARCHAR(36)"),
        ("image_job_batches", "user_id",
         "ALTER TABLE image_job_batches ADD COLUMN IF NOT EXISTS user_id VARCHAR(36)"),
        # v530: source discriminator on image_variants — 'ai' | 'manual'.
        ("image_variants", "source",
         "ALTER TABLE image_variants ADD COLUMN IF NOT EXISTS source VARCHAR(16) DEFAULT 'ai' NOT NULL"),
        # v573: ingredient-type discriminator on image_edges (parallels
        # the SQLite migration above).
        ("image_edges", "kind",
         "ALTER TABLE image_edges ADD COLUMN IF NOT EXISTS kind VARCHAR(32)"),
    ]

    # v479: widen ImageJobBatch string columns to TEXT. The previous
    # VARCHAR caps (name=300, persona=200, setting=300, structure=300)
    # were arbitrary and caused:
    #   DataError: value too long for type character varying(300)
    # on imports with long Setting or Persona descriptions. These fields
    # are free-form metadata parsed from user markdown — capping them at
    # a fixed length is wrong. ALTER COLUMN TYPE TEXT is idempotent; if
    # the column is already TEXT, PostgreSQL returns without changes.
    # Only applies to Postgres; SQLite's VARCHAR has no length enforcement.
    #
    # v484: also widen promoted_video_job_id from INTEGER to VARCHAR(36).
    # Older deployments created this column as INTEGER (back when video
    # job IDs were numeric). Current code uses UUID strings, so any
    # stamp attempt raised:
    #   psycopg2.errors.InvalidTextRepresentation: invalid input syntax
    #   for type integer: "ba093cbb-900c-4a08-85d8-d0299dfee266"
    # The `USING` clause casts existing values (almost all NULL since
    # no stamp ever succeeded before); any non-NULL integers convert to
    # their text representation, which won't match real UUID job ids
    # but won't break anything either.
    postgres_type_changes = [
        ("image_job_batches", "name",       "ALTER TABLE image_job_batches ALTER COLUMN name       TYPE TEXT"),
        ("image_job_batches", "persona",    "ALTER TABLE image_job_batches ALTER COLUMN persona    TYPE TEXT"),
        ("image_job_batches", "setting",    "ALTER TABLE image_job_batches ALTER COLUMN setting    TYPE TEXT"),
        ("image_job_batches", "structure",  "ALTER TABLE image_job_batches ALTER COLUMN structure  TYPE TEXT"),
        ("image_job_batches", "promoted_video_job_id",
         "ALTER TABLE image_job_batches ALTER COLUMN promoted_video_job_id "
         "TYPE VARCHAR(36) USING promoted_video_job_id::text"),
        # v489: widen scene-metadata VARCHARs on image_nodes. Same bug
        # class as v479 — arbitrary caps on free-form markdown metadata.
        # rhythm_tier (was VARCHAR(100)) specifically caused
        # StringDataRightTruncation on imports with descriptive rhythm
        # labels. Idempotent ALTER TYPE TEXT.
        ("image_nodes", "scene_transition",
         "ALTER TABLE image_nodes ALTER COLUMN scene_transition TYPE TEXT"),
        ("image_nodes", "clip_mode",
         "ALTER TABLE image_nodes ALTER COLUMN clip_mode TYPE TEXT"),
        ("image_nodes", "visual_register",
         "ALTER TABLE image_nodes ALTER COLUMN visual_register TYPE TEXT"),
        ("image_nodes", "rhythm_tier",
         "ALTER TABLE image_nodes ALTER COLUMN rhythm_tier TYPE TEXT"),
        # name_prefix held arbitrary user input ("name prefix:" from
        # import form) — widened to TEXT for consistency with other
        # free-form fields.
        ("image_job_batches", "name_prefix",
         "ALTER TABLE image_job_batches ALTER COLUMN name_prefix TYPE TEXT"),
        # ImageNode.name is formed as prefix + "Scene N" — grows with
        # prefix length. Cap of 200 was tight. Widen to TEXT.
        ("image_nodes", "name",
         "ALTER TABLE image_nodes ALTER COLUMN name TYPE TEXT"),
    ]

    migrations = sqlite_migrations if is_sqlite else postgres_migrations
    try:
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
    except Exception:
        existing_tables = []

    with engine.connect() as conn:
        for table, column, sql in migrations:
            if table not in existing_tables:
                # Table doesn't exist yet — create_all will make it with the
                # columns already in place, so skip.
                continue
            try:
                if is_sqlite:
                    result = conn.execute(text(f"PRAGMA table_info({table})"))
                    cols = [row[1] for row in result]
                    if column in cols:
                        continue
                conn.execute(text(sql))
                conn.commit()
                log.info(f"[image_platform] Migration: added {table}.{column}")
            except Exception as e:
                log.warning(f"[image_platform] Migration skipped {table}.{column}: {e}")

        # v479: widen VARCHAR columns on image_job_batches to TEXT so
        # long free-form markdown metadata doesn't trigger
        # StringDataRightTruncation. Idempotent on Postgres — if the
        # column is already TEXT, ALTER TYPE TEXT is a no-op. Skipped
        # entirely on SQLite where VARCHAR has no enforced limit anyway.
        if not is_sqlite:
            for table, column, sql in postgres_type_changes:
                if table not in existing_tables:
                    continue
                try:
                    conn.execute(text(sql))
                    conn.commit()
                    log.info(f"[image_platform] Migration: widened {table}.{column} → TEXT")
                except Exception as e:
                    log.warning(f"[image_platform] Migration skipped {table}.{column} widen: {e}")

    # v447: backfill user_id on existing rows after the column exists
    _backfill_user_id_ownership()


def _backfill_user_id_ownership():
    """Assign any NULL user_id rows to the first active user.

    After v447 adds the user_id column, existing nodes + batches have
    user_id=NULL. On a single-user install that's effectively the original
    owner. Rather than leave them as "global" forever (a privacy leak if
    another user ever joins), stamp them with the first active user so
    they belong to someone specific.

    If no users exist yet (fresh install), skip — nothing to backfill.
    """
    try:
        from models import User  # lazy import; models.py may not be loaded at call time
        with get_db() as db:
            first_user = db.query(User).filter(User.is_active == True).order_by(User.created_at.asc()).first()  # noqa: E712
            if first_user is None:
                log.info("[image_platform] Backfill skipped: no users in DB yet")
                return

            # Stamp NULL rows with first_user.id
            n_nodes = db.query(ImageNode).filter(ImageNode.user_id.is_(None)).count()
            if n_nodes:
                db.query(ImageNode).filter(ImageNode.user_id.is_(None)).update(
                    {"user_id": first_user.id}, synchronize_session=False
                )
                log.info(f"[image_platform] Backfilled {n_nodes} nodes → user {first_user.id} ({first_user.email or first_user.name or '?'})")

            n_batches = db.query(ImageJobBatch).filter(ImageJobBatch.user_id.is_(None)).count()
            if n_batches:
                db.query(ImageJobBatch).filter(ImageJobBatch.user_id.is_(None)).update(
                    {"user_id": first_user.id}, synchronize_session=False
                )
                log.info(f"[image_platform] Backfilled {n_batches} batches → user {first_user.id}")

            if n_nodes or n_batches:
                db.commit()
    except Exception as e:
        log.warning(f"[image_platform] Backfill failed (non-fatal): {e}")


def cleanup_orphan_nodes():
    """Remove variants (and nodes whose only variant was orphaned) whose
    image files no longer exist on disk AND cannot be restored from R2.

    Called once at startup. If R2 storage is configured, tries to restore
    each missing file before declaring it lost.

    v466: previously this function *downloaded* every missing variant
    from R2 synchronously at startup, blocking the webapp from serving
    traffic until complete. On Render's ephemeral filesystem, every
    restart triggered a full re-download of every variant → 30s-5min
    startup delay → health-check failure → 502 Bad Gateway → restart
    loop. Fix: only check if R2 HAS the object (HEAD request, no body),
    don't actually download. The /files/{token} endpoint already does
    lazy rehydration when a variant is actually requested, so eager
    download at startup is wasted work anyway.

    Returns count of nodes removed + variants removed.
    """
    try:
        storage = _storage_or_none()
        storage_available = storage is not None
        with get_db() as db:
            all_variants = db.query(ImageVariant).all()
            removed_variants = 0
            verified_in_r2 = 0
            for v in all_variants:
                abs_path = images_root() / v.image_path
                if abs_path.exists():
                    continue
                # File is missing locally. Check if it's in R2 WITHOUT
                # downloading — just a cheap HEAD request. If yes, the
                # variant is recoverable; /files/{token} will rehydrate
                # it on demand.
                exists_in_r2 = False
                if storage_available:
                    try:
                        key = _r2_key_for(v.image_path)
                        # boto3 head_object — fast, no body transfer
                        storage.client.head_object(Bucket=storage.bucket_name, Key=key)
                        exists_in_r2 = True
                    except Exception:
                        exists_in_r2 = False
                if exists_in_r2:
                    verified_in_r2 += 1
                    continue
                # Truly orphaned — not on disk, not in R2
                log.info(f"[image_platform] Cleanup: variant {v.id} file missing and no R2 backup ({v.image_path})")
                db.delete(v)
                removed_variants += 1
            db.flush()
            if verified_in_r2:
                log.info(f"[image_platform] Cleanup: verified {verified_in_r2} variants are recoverable from R2 (not downloading — will rehydrate lazily)")

            # Now find nodes with no variants at all
            all_nodes = db.query(ImageNode).all()
            removed_nodes = 0
            for n in all_nodes:
                remaining = [v for v in n.variants if v.id]
                if len(remaining) == 0 and n.kind == "upload":
                    # Upload node with no variant — the file is gone, can't recover
                    log.info(f"[image_platform] Cleanup: upload node {n.id} has no variants left — deleting")
                    # Detach children first
                    for e in db.query(ImageEdge).filter(ImageEdge.parent_node_id == n.id).all():
                        db.delete(e)
                    db.delete(n)
                    removed_nodes += 1
                elif len(remaining) == 0 and n.kind == "generated":
                    # Generated node with lost variants — can be regenerated, keep node
                    # but reset state
                    if n.status == "ready":
                        n.status = "draft"
                        n.chosen_variant_id = None
                        log.info(f"[image_platform] Cleanup: generated node {n.id} lost its variants — reset to draft")
                elif n.chosen_variant_id and not any(v.id == n.chosen_variant_id for v in remaining):
                    # Chosen variant was among the deleted
                    n.chosen_variant_id = None
                    if n.status == "ready":
                        n.status = "draft"

            db.commit()
            if removed_variants or removed_nodes or verified_in_r2:
                log.info(
                    f"[image_platform] Cleanup complete: {verified_in_r2} verified in R2 (lazy rehydration), "
                    f"{removed_variants} orphan variants removed, {removed_nodes} orphan nodes removed"
                )
            return {
                "removed_variants": removed_variants,
                "removed_nodes": removed_nodes,
                "verified_in_r2": verified_in_r2,
            }
    except Exception as e:
        log.exception(f"[image_platform] Cleanup failed: {e}")
        return {"error": str(e)}


# =============================================================================
# Paths
# =============================================================================

def _data_root() -> Path:
    return Path(app_config.data_dir)


def images_root() -> Path:
    """Root folder for generated/uploaded image files."""
    p = _data_root() / "images"
    p.mkdir(parents=True, exist_ok=True)
    return p


def uploads_root() -> Path:
    p = images_root() / "uploads"
    p.mkdir(parents=True, exist_ok=True)
    return p


def node_dir(node_id: int) -> Path:
    p = images_root() / f"node_{node_id}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def jobs_watch_dir() -> Path:
    """Watch folder where the image worker picks up jobs.

    image_worker.py --watch <this path>
    """
    p = _data_root() / "_image_jobs"
    p.mkdir(parents=True, exist_ok=True)
    return p


# =============================================================================
# Object storage (R2/S3) helpers — makes image files survive Render redeploys
# =============================================================================
# On Render the filesystem is ephemeral, so every redeploy wipes /app/data.
# If the backends.storage module is configured (S3_ENDPOINT etc. set), we
# also back up every saved image to R2 and restore from R2 on serve-miss.
# When not configured, everything falls back to local-only — fine for dev.

_R2_PREFIX = "image_platform"  # all image files live under this prefix in the bucket


def _storage_or_none():
    """Return an ObjectStorage instance if configured, else None."""
    try:
        from backends.storage import get_storage, is_storage_configured
        if is_storage_configured():
            return get_storage()
    except Exception:
        pass
    return None


def _r2_key_for(rel_path: str) -> str:
    """Convert a relative-to-images_root path into an R2 object key."""
    # Normalize to forward slashes
    rel = str(rel_path).replace("\\", "/").lstrip("/")
    return f"{_R2_PREFIX}/{rel}"


def _storage_upload_file(local_path: Path, rel_path: str) -> bool:
    """Upload a file to R2 if configured. Returns True if uploaded."""
    storage = _storage_or_none()
    if storage is None:
        return False
    try:
        key = _r2_key_for(rel_path)
        ext = local_path.suffix.lower()
        content_type = {".png": "image/png", ".jpg": "image/jpeg",
                        ".jpeg": "image/jpeg", ".webp": "image/webp"}.get(ext, "application/octet-stream")
        storage.upload_file(str(local_path), key, content_type=content_type)
        # v523.2: evict from the known-missing cache so a fresh poll
        # after this upload sees the file. Without this, a path that
        # was previously checked-and-missing (e.g. before a regenerate)
        # would still be served as 404 from the cache even though the
        # actual R2 object now exists.
        _r2_known_missing.discard(rel_path)
        return True
    except Exception as e:
        log.warning(f"[image_platform] R2 upload failed for {rel_path}: {e}")
        return False


def _storage_download_to_local(rel_path: str) -> bool:
    """Download a file from R2 to its local path. Returns True on success.

    v523.2: distinguish between "file not in R2" (a 404 / NoSuchKey error,
    which is expected when a variant was rejected by Flow's content
    classifier or when ImageVariant rows became stale after a partial
    regeneration) and "real R2 error" (network issue, auth issue, etc.).
    The 404 case is logged at DEBUG level — frontends and video
    pipelines can poll missing variants for legitimate reasons (e.g.
    while a regeneration is in flight), and the 404 logs were filling
    production logs without indicating a real problem.
    """
    if rel_path in _r2_known_missing:
        # v523.2: short-circuit known-missing paths. They've already been
        # checked once this process and confirmed not in R2. Skip the
        # network round-trip on subsequent polls.
        return False
    storage = _storage_or_none()
    if storage is None:
        return False
    try:
        key = _r2_key_for(rel_path)
        local_path = images_root() / rel_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        storage.download_file(key, str(local_path))
        log.info(f"[image_platform] Restored from R2: {rel_path}")
        return True
    except Exception as e:
        # v523.2: classify the error. boto3 raises ClientError with
        # response['Error']['Code'] == '404' (HeadObject) or 'NoSuchKey'
        # (GetObject) when the object doesn't exist. Other errors —
        # auth failures, throttling, network issues — still log at
        # WARNING because they may indicate a real problem.
        msg = str(e)
        is_not_found = (
            "Not Found" in msg
            or "NoSuchKey" in msg
            or "(404)" in msg
        )
        if is_not_found:
            log.debug(f"[image_platform] R2 download skipped (not in R2): {rel_path}")
            _r2_known_missing.add(rel_path)
        else:
            log.warning(f"[image_platform] R2 download failed for {rel_path}: {e}")
        return False


# v523.2: per-process cache of R2 keys we've confirmed are not present.
# Stops repeated HeadObject roundtrips when the frontend or video
# pipeline polls for the same missing variant. Cleared by:
#   - process restart (intentional — fresh check after deploy)
#   - explicit eviction when a new file is uploaded to that key
#     (handled in _storage_upload_file)
_r2_known_missing: Set[str] = set()


def _storage_delete(rel_path: str):
    """Delete a file from R2 if configured. Silently ignores errors."""
    storage = _storage_or_none()
    if storage is None:
        return
    try:
        key = _r2_key_for(rel_path)
        # Use the underlying boto3 client directly — ObjectStorage may not
        # expose delete. Fall back to client if method missing.
        if hasattr(storage, "delete_file"):
            storage.delete_file(key)
        elif hasattr(storage, "client"):
            storage.client.delete_object(Bucket=storage.bucket_name, Key=key)
    except Exception as e:
        log.warning(f"[image_platform] R2 delete failed for {rel_path}: {e}")


# =============================================================================
# Models
# =============================================================================

# Node status values:
#   draft      — created but not yet generated
#   queued     — job written to watch folder, worker hasn't picked up yet
#   generating — worker is actively running
#   ready      — variants present, may or may not have a chosen one
#   failed     — last generation attempt failed (see error_message)


class ImageNode(Base):
    """A single image-generation request in the graph.

    If kind='upload', the node represents a user-uploaded reference image.
    It has no prompt, one variant (the uploaded file), status=ready, and
    is a leaf of the graph (cannot be regenerated).
    """
    __tablename__ = "image_nodes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Owner scoping — every node belongs to the user who created it.
    # Nullable at the DB level during the v447 migration window; new code
    # must always stamp it. Backfilled at startup to the first active user.
    user_id = Column(String(36), nullable=True, index=True)
    name = Column(Text, nullable=True)  # v489: widened from String(200) — grows with user's name_prefix
    kind = Column(String(20), default="generated", nullable=False)  # generated | upload
    prompt = Column(Text, nullable=True)
    aspect_ratio = Column(String(20), default="9:16")
    resolution = Column(String(10), default="2K")
    model = Column(String(50), default="nano_banana_2")
    n_variants = Column(Integer, default=4)

    status = Column(String(20), default="draft", nullable=False)
    chosen_variant_id = Column(Integer, ForeignKey("image_variants.id", use_alter=True, name="fk_chosen_variant"), nullable=True)
    error_message = Column(Text, nullable=True)

    # Worker claim (HTTP-pull mode). When a remote worker picks up a job it
    # stamps these. Used to release stale claims.
    claimed_by_worker = Column(String(100), nullable=True)
    claimed_at = Column(DateTime, nullable=True)

    # Scene-table import metadata. Populated by import_scene_table for every
    # scene node so the Job Overview UI (and the later "Promote to video"
    # feature) can present structured per-scene info without re-parsing the
    # source markdown. All nullable — existing nodes predating this schema
    # just have NULL values.
    #
    # v489: widened from String(N) to Text. These fields carry free-form
    # descriptive metadata parsed from user markdown. The previous caps
    # (scene_transition=50, clip_mode=50, visual_register=50,
    # rhythm_tier=100) were arbitrary and caused
    # `StringDataRightTruncation: value too long for type character
    # varying(100)` on imports. Same class of bug that v479 fixed for
    # ImageJobBatch. Text has no length limit in PostgreSQL and no
    # meaningful performance difference for values under a few KB.
    batch_id = Column(String(36), nullable=True, index=True)   # UUID of the import batch
    scene_index_in_batch = Column(Integer, nullable=True)      # 0, 1, 2, ...
    voiceover_text = Column(Text, nullable=True)               # the "text:" field
    scene_transition = Column(Text, nullable=True)             # null / cut / blend / ...
    clip_mode = Column(Text, nullable=True)                    # blend / fresh
    visual_register = Column(Text, nullable=True)              # HOOK / RECIPE / ANATOMY / ...
    rhythm_tier = Column(Text, nullable=True)                  # authority (17w) etc.
    action_note = Column(Text, nullable=True)                  # description of in-frame motion
    # v537: explicit declaration of who speaks the dialogue line in this
    # scene. NULL or 'auto' = run _detect_voiceover_only (legacy behavior,
    # uses textual signals from image_prompt). 'on-camera' = main character
    # is the visible speaker, lip-sync ON. 'voiceover' = off-screen narrator
    # speaks while visible subject(s) stay silent with closed lips. The
    # writer should set this explicitly per-scene; auto-detect is fragile
    # because it relies on phrase-match heuristics that miss anything not
    # in the hardcoded signal lists (e.g., "his shoulders" instead of
    # "her shoulders" misses face-on-camera detection).
    speaker_mode = Column(String(20), nullable=True)

    # v667: transformation-video metadata copied from the decode artifact.
    # frame_anchor_s = source-video timestamp this image is anchored to
    # (seconds, float). visual_delta = one-line description of the change
    # vs the prior chained image. narrative_lens = optional lens label
    # ("transformation-state-2", "hook-state", etc.). All NULL on pre-v667
    # imports — the lift-side composer falls back to whisper-VAD trim.
    frame_anchor_s = Column(Float, nullable=True)
    visual_delta = Column(Text, nullable=True)
    narrative_lens = Column(String(40), nullable=True)

    # v681: per-image cast presence (parallel to ImageSceneAssignment.cast_json
    # but reflects what the IMAGE prompt depicts). NULL = scan prompt for
    # ingredient names (v509 fallback). JSON array of Ingredients Name
    # strings when present.
    cast_json = Column(Text, nullable=True)

    # v572: per-clip Veo prompt overrides — when non-NULL, build_prompt
    # is bypassed and the prebuilt prompt is shipped to Veo verbatim.
    # These two columns are the DENORMALIZED first-clip overrides for
    # the UI thumbnail card, parallel to action_note / voiceover_text.
    # The authoritative storage of multi-line overrides is in
    # ImageSceneAssignment.veo_prompts_json (a JSON list parallel to
    # lines_json). NULL on either column means "no override on the
    # first clip" — the auto-build path runs as before.
    veo_prompt_override = Column(Text, nullable=True)
    veo_negative_prompt_override = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    variants = relationship(
        "ImageVariant",
        back_populates="node",
        cascade="all, delete-orphan",
        foreign_keys="ImageVariant.node_id",
    )
    # Edges where this node is the child (its parents)
    parent_edges = relationship(
        "ImageEdge",
        back_populates="child",
        cascade="all, delete-orphan",
        foreign_keys="ImageEdge.child_node_id",
    )
    # Edges where this node is the parent (its children — used for dependency checks)
    child_edges = relationship(
        "ImageEdge",
        back_populates="parent",
        foreign_keys="ImageEdge.parent_node_id",
    )

    def to_dict(self, include_variants: bool = True) -> Dict[str, Any]:
        chosen = None
        if self.chosen_variant_id:
            for v in self.variants:
                if v.id == self.chosen_variant_id:
                    chosen = v.to_dict()
                    break

        # Count children currently waiting on a variant-choice from this node.
        # A child is "blocked by me" if it's in draft status — since import
        # puts scenes in draft only when a parent isn't ready/chosen.
        blocked_children_count = 0
        try:
            for edge in self.child_edges:
                child = edge.child
                if child is not None and child.status == "draft":
                    blocked_children_count += 1
        except Exception:
            pass

        data = {
            "id": self.id,
            "name": self.name or (f"Node {self.id}" if self.kind == "generated" else f"Upload {self.id}"),
            "kind": self.kind,
            "prompt": self.prompt,
            "aspect_ratio": self.aspect_ratio,
            "resolution": self.resolution,
            "model": self.model,
            "n_variants": self.n_variants,
            "status": self.status,
            "chosen_variant_id": self.chosen_variant_id,
            "chosen_variant": chosen,
            "error_message": self.error_message,
            "blocked_children_count": blocked_children_count,
            # Scene-table import metadata (nullable — only populated when
            # the node was created via import_scene_table)
            "batch_id": self.batch_id,
            "scene_index_in_batch": self.scene_index_in_batch,
            "voiceover_text": self.voiceover_text,
            "scene_transition": self.scene_transition,
            "clip_mode": self.clip_mode,
            "visual_register": self.visual_register,
            "rhythm_tier": self.rhythm_tier,
            "action_note": self.action_note,
            "speaker_mode": self.speaker_mode,  # v537
            "veo_prompt_override": self.veo_prompt_override,                    # v572
            "veo_negative_prompt_override": self.veo_negative_prompt_override,  # v572
            # v667 — transformation-video metadata.
            "frame_anchor_s": self.frame_anchor_s,
            "visual_delta": self.visual_delta,
            "narrative_lens": self.narrative_lens,
            # v681 — per-image cast (decoded list of Ingredients Name strings).
            "cast": (json.loads(self.cast_json) if self.cast_json else None),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
        if include_variants:
            data["variants"] = [v.to_dict() for v in sorted(self.variants, key=lambda x: x.variant_index or 0)]
            data["parents"] = [e.to_dict() for e in sorted(self.parent_edges, key=lambda x: x.slot_order or 0)]
        return data


class ImageVariant(Base):
    """One generated image (one of N variants from a single generation).

    v530: variants now carry a `source` discriminator. AI generations
    set source='ai'; user drag-drop uploads set source='manual'. The
    rest of the schema is identical — manual variants are first-class
    peers to AI variants in selection, reference chaining, and serving.
    """
    __tablename__ = "image_variants"

    id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(Integer, ForeignKey("image_nodes.id", ondelete="CASCADE"), nullable=False)
    variant_index = Column(Integer, default=1)  # 1..N
    image_path = Column(String(500), nullable=False)  # relative to images_root()
    # v530: 'ai' | 'manual'. Default 'ai' so existing rows backfill correctly.
    source = Column(String(16), nullable=False, default='ai')
    created_at = Column(DateTime, default=datetime.utcnow)

    node = relationship("ImageNode", back_populates="variants", foreign_keys=[node_id])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "node_id": self.node_id,
            "variant_index": self.variant_index,
            # ?v={id} forces the browser to treat each regenerated variant as
            # a distinct resource. Without it, regenerating a node produces
            # new bytes at the same file path ("nodes/N/variant_1.png") and
            # the old bytes stay pinned in the browser's HTTP cache.
            "image_url": f"/api/images/files/{self.image_path}?v={self.id}",
            # v530: source = 'ai' or 'manual'. UI uses this to render the
            # 'M' badge / folder icon on manual variants.
            "source": self.source or 'ai',
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class ImageEdge(Base):
    """Parent→child relationship. A child can have up to 3 parents (slot 0/1/2).

    role is a free-form label ("subject", "background", "ref_1", ...).
    When a child node is generated, the chosen variant of each parent is
    passed as a reference image to the worker, ordered by slot_order.

    v573: `kind` discriminates upload-backed edges by ingredient type
    (`character` for persona uploads, `product` for product uploads).
    Leaves NULL for chain edges, anchor-scene edges, and pre-v573 rows.
    Used by `_build_flow_prompt_with_manifest` to emit the right role
    line per slot ("Use Image N for the main character..." vs "Use Image
    N for the product's label..."), without which Flow blends references
    as generic visual context (causing the persona-body-bleeding-into-
    scene failure mode).
    """
    __tablename__ = "image_edges"

    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_node_id = Column(Integer, ForeignKey("image_nodes.id", ondelete="CASCADE"), nullable=False)
    child_node_id = Column(Integer, ForeignKey("image_nodes.id", ondelete="CASCADE"), nullable=False)
    role = Column(String(50), nullable=True)
    slot_order = Column(Integer, default=0)  # 0, 1, 2
    # v573: ingredient type discriminator — see class docstring.
    kind = Column(String(32), nullable=True)

    parent = relationship("ImageNode", back_populates="child_edges", foreign_keys=[parent_node_id])
    child = relationship("ImageNode", back_populates="parent_edges", foreign_keys=[child_node_id])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "parent_node_id": self.parent_node_id,
            "child_node_id": self.child_node_id,
            "role": self.role,
            "slot_order": self.slot_order,
            "kind": self.kind,
        }


class ImageWorkerHeartbeat(Base):
    """Per-worker heartbeat timestamp — stored in DB so the online
    indicator works correctly in multi-process webapp deployments
    (uvicorn --workers N on Render). In-memory state would only live in
    one process, making the status lookup racy across requests."""
    __tablename__ = "image_worker_heartbeats"

    worker_id = Column(String(100), primary_key=True)
    last_heartbeat_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ImageJobBatch(Base):
    """One row per scene-table import. Stores doc-level metadata and the
    raw markdown so we can re-promote to video if needed. Individual
    ImageNode rows in the batch reference this via batch_id.
    """
    __tablename__ = "image_job_batches"

    id = Column(String(36), primary_key=True)  # UUID string
    # Owner scoping (v447). Nullable during migration window; new code stamps it.
    user_id = Column(String(36), nullable=True, index=True)
    # v479: widened from String(300)/String(200) to Text. These fields
    # carry free-form descriptive metadata parsed from user markdown
    # (persona can be "Pops Hollis (male-audience extension — strength-
    # lost-can-be-reclaimed register)", setting can be a multi-sentence
    # location description, structure can be an option description). The
    # previous VARCHAR(300) cap was an arbitrary guess that caused
    # `psycopg2.errors.StringDataRightTruncation` crashes on perfectly
    # valid imports. Text has no length limit in PostgreSQL and no
    # meaningful performance difference for values under a few KB.
    name = Column(Text, nullable=True)
    source_markdown = Column(Text, nullable=True)  # full raw md
    persona = Column(Text, nullable=True)
    setting = Column(Text, nullable=True)
    duration_seconds = Column(Integer, nullable=True)
    structure = Column(Text, nullable=True)
    total_scenes = Column(Integer, default=0, nullable=False)
    name_prefix = Column(Text, nullable=True)  # what the user typed in the importer (v489: widened from String(100))
    subject_node_id = Column(Integer, nullable=True)
    promoted_video_job_id = Column(String(36), nullable=True)  # set once promoted/prepared
    # Video-tab presentation hints parsed from md (both optional).
    # video_mode: "storyboard" | "auto-cycle" | "simple". Default: storyboard.
    # auto_split: whether the dialogue editor's auto-split toggle should be
    # ON when the video-tab lands from a prepare-for-video call. Default: False.
    video_mode = Column(String(20), nullable=True)
    auto_split = Column(Boolean, default=False, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "persona": self.persona,
            "setting": self.setting,
            "duration_seconds": self.duration_seconds,
            "structure": self.structure,
            "total_scenes": self.total_scenes,
            "name_prefix": self.name_prefix,
            "subject_node_id": self.subject_node_id,
            "promoted_video_job_id": self.promoted_video_job_id,
            "video_mode": self.video_mode or "storyboard",
            "auto_split": bool(self.auto_split) if self.auto_split is not None else False,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class ImageSceneAssignment(Base):
    """One row per video-storyboard scene. Separate from ImageNode — a scene
    is a *video* construct (an image + one or more dialogue lines + a
    clip_mode + a transition). An ImageNode is an image-generation construct.

    Multiple scenes can reference the same ImageNode (image reuse across
    scenes). A scene can cover one or many dialogue lines.

    For old-format imports (no ``### Image N`` / ``### Scene N`` separation),
    these rows are synthesized 1:1 during the import so the promote-to-video
    flow has a uniform data shape regardless of md format.
    """
    __tablename__ = "image_scene_assignments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(36), ForeignKey("image_job_batches.id"), nullable=False, index=True)
    scene_index = Column(Integer, nullable=False)      # 0, 1, 2, ...
    # v681 — nullable for text_card scenes (no Nano Banana 2 image render).
    # Pre-v681 every scene had an image binding; new optional bullet
    # `scene_type: text_card` allows scene rows with image_node_id=NULL,
    # rendered via ffmpeg drawtext at export time.
    image_node_id = Column(Integer, ForeignKey("image_nodes.id"), nullable=True)
    clip_mode = Column(String(20), default="blend")   # blend | fresh | continue
    transition = Column(String(20), nullable=True)    # cut | blend | null
    # JSON arrays, parallel to each other. len(lines_json) == len(action_notes_json).
    # action_notes_json entries may be null for lines that don't have one.
    lines_json = Column(Text, nullable=False, default="[]")
    action_notes_json = Column(Text, nullable=True)
    # v572: parallel array of per-line Veo prompt overrides. JSON list,
    # same length as lines_json. Each entry is null (no override → use
    # build_prompt as before) OR an object {text_prompt, negative_prompt}.
    # Column-level NULL = no overrides anywhere in this scene; column
    # populated with all-null entries = no overrides per-line, equivalent
    # but stored explicitly. The platform tolerates both shapes.
    veo_prompts_json = Column(Text, nullable=True)
    # v644: parallel array of per-line audio-padding suffixes. JSON list,
    # same length as lines_json. Each entry is null (no pad → Veo prompt
    # uses bare line) OR a string appended after the line in the Veo
    # prompt only. Whisper-VAD continues to use the bare line as script
    # truth, so the pad's spoken audio is automatically trimmed by the
    # existing apply_vad pipeline as unmatched filler. Column-level
    # NULL = no pads anywhere; populated with all-null entries =
    # equivalent. Migration: nullable column, no backfill needed.
    pads_json = Column(Text, nullable=True)
    # v668: per-scene cut mode (whisper | timeline | auto). NULL → defaults
    # to legacy whisper-VAD behavior. Distinct from clip_mode (which controls
    # Veo render strategy: blend/fresh/continue) — cut_mode controls the
    # post-render trim strategy on the rendered clip.
    cut_mode = Column(String(20), nullable=True)
    # v681: multi-character cast model + text-card scene type.
    # cast_json = JSON array of Ingredients Name strings present in this
    # scene; when non-empty, image worker binds ONLY these (skipping v509
    # prompt-scan). scene_type ∈ {None|'shot'|'text_card'} — text_card
    # bypasses Nano Banana 2 + Veo. caption captures source caption text
    # (decode-only on shot scenes; rendered text on text_card scenes).
    # bg_color / duration_s used by ffmpeg drawtext renderer for text_card.
    cast_json = Column(Text, nullable=True)
    scene_type = Column(String(20), nullable=True)
    caption = Column(Text, nullable=True)
    bg_color = Column(String(20), nullable=True)
    duration_s = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        import json as _json
        try:
            lines = _json.loads(self.lines_json or "[]")
        except Exception:
            lines = []
        try:
            notes = _json.loads(self.action_notes_json or "null")
            if notes is None:
                notes = [None] * len(lines)
        except Exception:
            notes = [None] * len(lines)
        # Normalize note length to match lines count
        while len(notes) < len(lines):
            notes.append(None)
        notes = notes[:len(lines)]
        # v572: per-line Veo prompt overrides, parallel to lines + notes.
        try:
            veo_prompts = _json.loads(self.veo_prompts_json or "null")
            if veo_prompts is None:
                veo_prompts = [None] * len(lines)
        except Exception:
            veo_prompts = [None] * len(lines)
        while len(veo_prompts) < len(lines):
            veo_prompts.append(None)
        veo_prompts = veo_prompts[:len(lines)]
        # v644: per-line audio-padding suffixes (Veo-prompt-only; whisper
        # script uses the bare line so apply_vad trims pad audio).
        try:
            pads = _json.loads(self.pads_json or "null")
            if pads is None:
                pads = [None] * len(lines)
        except Exception:
            pads = [None] * len(lines)
        while len(pads) < len(lines):
            pads.append(None)
        pads = pads[:len(lines)]
        return {
            "id": self.id,
            "batch_id": self.batch_id,
            "scene_index": self.scene_index,
            "image_node_id": self.image_node_id,
            "clip_mode": self.clip_mode or "blend",
            "transition": self.transition,
            "lines": lines,
            "action_notes": notes,
            "veo_prompts": veo_prompts,  # v572
            "pads": pads,  # v644
            "cut_mode": self.cut_mode,  # v668 — whisper | timeline | auto | None
            # v681 — multi-character cast + text-card metadata.
            "cast": (_json.loads(self.cast_json) if self.cast_json else None),
            "scene_type": self.scene_type,
            "caption": self.caption,
            "bg_color": self.bg_color,
            "duration_s": self.duration_s,
        }


# =============================================================================
# Pydantic schemas
# =============================================================================

class ParentRef(BaseModel):
    parent_node_id: int
    role: Optional[str] = None
    slot_order: int = 0


class CreateNodeRequest(BaseModel):
    name: Optional[str] = None
    prompt: str
    aspect_ratio: str = "9:16"
    resolution: str = "2K"
    model: str = "nano_banana_2"
    n_variants: int = Field(4, ge=1, le=4)
    parents: List[ParentRef] = []


class UpdateNodeRequest(BaseModel):
    name: Optional[str] = None
    prompt: Optional[str] = None
    aspect_ratio: Optional[str] = None
    resolution: Optional[str] = None
    model: Optional[str] = None
    n_variants: Optional[int] = Field(None, ge=1, le=4)
    parents: Optional[List[ParentRef]] = None


class ChooseVariantRequest(BaseModel):
    variant_id: int


# =============================================================================
# Worker bridge (watch folder)
# =============================================================================

def _resolve_parent_image_inputs(db: Session, node: ImageNode) -> List[Dict[str, Any]]:
    """For each parent edge (ordered by slot), return a dict
    {"path": ..., "role": ..., "slot_order": ...} for the parent's chosen
    variant. Raises if any parent isn't ready yet.

    v509: returns role+slot_order alongside the path so the worker can
    use the role as a semantic ingredient name (e.g. "the main character",
    "her daughter", "the salvora bottle"). The worker slugifies the role
    into the upload basename so Flow's gallery alt-text becomes the
    semantic name, giving the prompt's ingredient references a strong
    matching signal. See image_worker_reference_analysis.md for the
    full design rationale.

    v477: if a parent file is missing from local disk, attempt R2
    rehydration before failing. On Render's ephemeral filesystem the
    uploads/ directory gets wiped on every redeploy, and a regenerate
    that happens post-deploy would otherwise 500 with "Parent image
    file missing" even though the file exists in R2. This mirrors the
    lazy-rehydration pattern used by serve_image_file at /files/{token}.
    """
    inputs: List[Dict[str, Any]] = []
    for edge in sorted(node.parent_edges, key=lambda e: e.slot_order or 0):
        parent: ImageNode = edge.parent
        if parent is None:
            raise HTTPException(400, f"Parent node {edge.parent_node_id} no longer exists")
        if parent.status != "ready" or parent.chosen_variant_id is None:
            raise HTTPException(
                400,
                f"Parent node {parent.id} is not ready (status={parent.status}, chosen={parent.chosen_variant_id})"
            )
        # Find the chosen variant
        chosen = next((v for v in parent.variants if v.id == parent.chosen_variant_id), None)
        if chosen is None:
            raise HTTPException(400, f"Parent node {parent.id} chosen variant not found")
        abs_path = images_root() / chosen.image_path
        if not abs_path.exists():
            # Try rehydrating from R2 before giving up. On Render's
            # ephemeral filesystem this happens routinely post-deploy.
            log.info(f"[image_platform] Parent file missing locally, attempting R2 restore: {chosen.image_path}")
            if _storage_download_to_local(chosen.image_path):
                # File is back on disk; continue.
                log.info(f"[image_platform] ✓ Restored parent from R2: {chosen.image_path}")
            else:
                # R2 doesn't have it either — permanent loss.
                raise HTTPException(
                    500,
                    f"Parent image file missing and not recoverable from R2: {chosen.image_path}. "
                    f"This typically means the file was uploaded before R2 storage was configured, "
                    f"or the R2 upload failed silently. The original upload cannot be recovered — "
                    f"you'll need to re-upload the parent reference and regenerate."
                )
            # Double-check it actually landed on disk
            if not abs_path.exists():
                raise HTTPException(
                    500,
                    f"R2 reported success but file still missing at {abs_path}"
                )
        inputs.append({
            "path": str(abs_path),
            "role": edge.role or "",
            "slot_order": edge.slot_order or 0,
        })
    return inputs


# Backwards-compat alias — some old code paths may import the old name.
def _resolve_parent_image_paths(db: Session, node: ImageNode) -> List[str]:
    """Legacy wrapper that returns just paths. Prefer
    _resolve_parent_image_inputs for new code."""
    return [item["path"] for item in _resolve_parent_image_inputs(db, node)]


def _classify_edge_for_manifest(edge: "ImageEdge") -> str:
    """v573: classify a parent edge for manifest-line generation.

    Returns one of: 'persona', 'product', 'chain', 'other'.

    Detection priority:
      1. `edge.kind` (set at import time for upload-backed edges in v573+):
         'character' → persona, 'product' → product.
      2. `edge.role` patterns: persona alias text, `variant_chain:*`,
         `chain_from_image_*`. These carry pre-v573 semantics for edges
         where `kind` is NULL.
      3. Fallback: 'other' (a named non-upload ingredient anchored to a
         prior scene).
    """
    role = (edge.role or "").strip()
    kind = (edge.kind or "").strip().lower()

    # Step 1: explicit kind from import-time ingredient type
    if kind == "character":
        return "persona"
    if kind == "product":
        return "product"

    # Step 2: role-based detection (covers pre-v573 rows + v573 chain edges)
    if role.startswith("variant_chain:"):
        # Variant chains bind to an upload-backed character base. In
        # practice this is always a persona variant (e.g. "her daughter
        # (before)" chained to "her daughter").
        return "persona"
    if role.startswith("chain_from_image_"):
        return "chain"
    if _is_persona_alias(role):
        return "persona"
    # v573 backstop for pre-v573 rows where `kind` is NULL: the legacy
    # single-subject path uses role="subject" for the persona edge and
    # role="reference" for the prior-scene chain edge.
    if role.lower() == "subject":
        return "persona"
    if role.lower() == "reference":
        return "chain"

    # Step 3: anchor-scene-bound named ingredients (e.g. a non-persona
    # secondary character mentioned in a prompt and anchored to its
    # first-mention scene).
    return "other"


def _resolve_flow_prompt_bindings(node: "ImageNode") -> str:
    """v581: explicit reference bindings are written into the markdown body
    by the author. The platform no longer prepends a manifest header.
    Instead, it does targeted slot substitution to translate the markdown's
    role-based and image-number references to Flow's actual slot positions.

    Three substitution patterns:

    1. ROLE-BASED PERSONA reference (always present, every image):
       "the uploaded character reference image"  →  "Image {N}"
       where N = persona's Flow slot + 1 (canonically 1)

    2. ROLE-BASED PRODUCT reference (only when product is bound):
       "the uploaded product reference image"  →  "Image {N}"
       where N = product's Flow slot + 1 (canonically 2)

    3. NUMBER-BASED CHAIN reference (only when reference_image is set):
       "Image {K}" in the chain binding line, where K is the markdown
       image number of the chain parent  →  "Image {M}", where M = chain's
       Flow slot + 1 (canonically 3 when persona+product+chain are bound,
       2 when only persona+chain).

    Returns the prompt body with role/slot references translated. Returns
    the prompt unchanged when the node has no parent edges (Image 1
    establishing scenes with reference_image: none and no product binding).

    Why this changed from v552/v573:
      Pre-v581 the binding was invisible in the markdown — the actual
      prompt sent to Banana 2 only existed in this function's prepend
      logic, and authors couldn't preview or audit it. Per v581 the
      binding is now in the markdown body where the author wrote it,
      visible to both human and machine. This function only handles
      the slot-translation step that requires runtime knowledge of
      Flow's positional slot ordering.
    """
    body = (node.prompt or "")
    edges = sorted(node.parent_edges or [], key=lambda e: e.slot_order or 0)
    if not edges or not body:
        return body

    import re as _re
    for i, edge in enumerate(edges):
        flow_image_num = i + 1
        cls = _classify_edge_for_manifest(edge)

        if cls == "persona":
            body = body.replace(
                "the uploaded character reference image",
                f"Image {flow_image_num}",
            )

        elif cls == "product":
            body = body.replace(
                "the uploaded product reference image",
                f"Image {flow_image_num}",
            )

        elif cls == "chain":
            # v589.1: PRIMARY substitution — semantic chain-reference
            # phrase. Author-written form is robust to direct paste-into-
            # Flow / Gemini-direct testing because the role descriptor
            # ("the prior-scene reference image") is meaningful even
            # without substitution; the platform translation makes it
            # match Banana 2's positional view of the inputs at emission.
            body = body.replace(
                "the prior-scene reference image",
                f"Image {flow_image_num}",
            )
            body = body.replace(
                "the previous scene's reference image",
                f"Image {flow_image_num}",
            )

            # v581 LEGACY substitution — number-based reference. Kept
            # for backward compatibility with pre-v589.1 markdowns.
            # New decodes should NOT use "Image K" by markdown number;
            # use the semantic phrase above instead.
            parent = edge.parent
            if parent is None or parent.scene_index_in_batch is None:
                continue
            md_image_num = parent.scene_index_in_batch + 1
            if md_image_num == flow_image_num:
                continue
            # Word-boundary substitution to avoid rewriting "Image 12"
            # when looking for "Image 1".
            pattern = rf"\bImage {md_image_num}\b"
            replacement = f"Image {flow_image_num}"
            body = _re.sub(pattern, replacement, body)

        else:
            # Anchor-scene-bound named ingredient — v581 has no canonical
            # binding line for these; the role mapping happens via the
            # edge being attached to its slot at worker emission.
            pass

    return body


def _build_flow_prompt_with_manifest(node: "ImageNode") -> str:
    """v581 alias kept for backward compatibility — internal callers
    have been migrated to _resolve_flow_prompt_bindings. External code
    that imported the old name still works.
    """
    return _resolve_flow_prompt_bindings(node)


def write_generation_job(db: Session, node: ImageNode) -> Path:
    """Write a job JSON file into the watch folder for the image worker.

    v509 job format (matches image_worker.py watch-folder contract):
      {
        "id": "node_<id>",
        "prompt": "...",
        "input_images": [
          {"path": "/abs/path1.png", "role": "the main character", "slot_order": 0},
          {"path": "/abs/path2.png", "role": "her daughter", "slot_order": 1},
          ...
        ],
        "output_dir": "/abs/path/to/node_<id>/",
        "aspect_ratio": "9:16",
        "resolution": "2K",
        "model": "nano_banana_2",
        "variants": 4
      }

    The worker accepts both the v509 dict format and the legacy
    list-of-strings format for backwards compat (see image_worker.py
    _process_watch_job).

    The worker writes back a .done.json alongside with:
      { "id": "...", "status": "completed"|"failed",
        "output_paths": ["variant_1.png", ...],  // relative to output_dir
        "error": null }
    """
    input_inputs = _resolve_parent_image_inputs(db, node)

    out_dir = node_dir(node.id)
    # Clean old variant files (safety — should already be empty at this point)
    for f in out_dir.glob("variant_*.png"):
        try:
            f.unlink()
        except Exception:
            pass

    job = {
        "id": f"node_{node.id}",
        # v573: prepend the per-slot reference manifest so Flow knows
        # what each uploaded reference is for. Falls through unchanged
        # for nodes with no parent edges (establishing Image 1 cases).
        "prompt": _resolve_flow_prompt_bindings(node),
        "input_images": input_inputs,
        "output_dir": str(out_dir),
        "aspect_ratio": node.aspect_ratio,
        "resolution": node.resolution,
        "model": node.model,
        "variants": int(node.n_variants or 1),
    }

    job_path = jobs_watch_dir() / f"node_{node.id}.json"
    job_path.write_text(json.dumps(job, indent=2), encoding="utf-8")
    log.info(f"[image_platform] Wrote job file: {job_path.name}  inputs={len(input_inputs)}  variants={job['variants']}")
    return job_path


async def watch_done_files_loop(stop_event: asyncio.Event):
    """Background task: poll the watch folder for .done.json files
    and update the corresponding nodes."""
    log.info("[image_platform] Watch-folder polling task started.")
    while not stop_event.is_set():
        try:
            await _poll_done_files_once()
        except Exception as e:
            log.exception(f"[image_platform] Poll error: {e}")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            pass
    log.info("[image_platform] Watch-folder polling task stopped.")


async def _poll_done_files_once():
    watch = jobs_watch_dir()
    # Read the list synchronously, but hand the DB work off to a thread
    # executor so we don't block the event loop with SQLAlchemy calls.
    done_files = list(watch.glob("node_*.done.json"))
    if not done_files:
        return
    loop = asyncio.get_event_loop()
    for done_file in done_files:
        try:
            await loop.run_in_executor(None, _process_done_file_sync, done_file)
        except Exception as e:
            log.exception(f"[image_platform] Failed to process {done_file.name}: {e}")


def _process_done_file_sync(done_file: Path):
    """Read a .done.json, update the corresponding node, and remove the files.

    Synchronous. Called from a thread-pool executor so it doesn't block
    the event loop.
    """
    # Parse node id from filename: node_<id>.done.json
    stem = done_file.stem  # "node_<id>.done"
    if stem.endswith(".done"):
        stem = stem[:-5]
    if not stem.startswith("node_"):
        return
    try:
        node_id = int(stem.split("_", 1)[1])
    except (ValueError, IndexError):
        return

    try:
        payload = json.loads(done_file.read_text(encoding="utf-8"))
    except Exception as e:
        log.error(f"[image_platform] Can't read {done_file.name}: {e}")
        # Remove unreadable file so it doesn't loop forever
        try:
            done_file.unlink()
        except Exception:
            pass
        return

    status = payload.get("status", "failed")
    error = payload.get("error")
    output_dir_raw = payload.get("output_dir")
    output_paths = payload.get("output_paths") or []

    # Apply to DB
    with get_db() as db:
        node = db.query(ImageNode).filter(ImageNode.id == node_id).first()
        if node is None:
            log.warning(f"[image_platform] .done.json for unknown node {node_id}")
            _cleanup_job_files(node_id)
            return

        output_dir = Path(output_dir_raw) if output_dir_raw else node_dir(node_id)

        if status == "completed":
            # v559: race-condition guard. If the user uploaded a manual
            # variant while this worker job was in flight (only possible
            # via the v559 queued-state upload tile), `node.chosen_variant_id`
            # already points to a manual variant. The user explicitly opted
            # out of AI generation for this node — discard the worker's
            # results entirely instead of cluttering their variant grid
            # with an unwanted AI variant. This is silent on the user side
            # (no toast / no log to UI) — just shows in server logs.
            chosen_v = next(
                (v for v in node.variants if v.id == node.chosen_variant_id),
                None,
            )
            if chosen_v is not None and getattr(chosen_v, 'source', 'ai') == 'manual':
                log.info(
                    f"[image_platform] Worker completed for node {node_id} but a "
                    f"manual variant is already chosen — discarding worker results "
                    f"({len(output_paths)} variant file(s) ignored). "
                    f"User opted out of AI generation mid-flight via manual upload."
                )
                # Best-effort cleanup of the worker's output files since
                # we're not adopting them as variants. Don't fail the
                # handler if cleanup misses anything.
                for rel_or_abs in output_paths:
                    try:
                        pp = Path(rel_or_abs)
                        if not pp.is_absolute():
                            pp = output_dir / pp
                        pp.unlink(missing_ok=True)
                    except Exception:
                        pass
                _cleanup_job_files(node_id)
                return

            # v559: preserve any manual variants when replacing — only
            # delete AI variants. The default behavior of "wipe every
            # variant on regenerate" was correct when manual uploads
            # didn't exist, but now they do, and we don't want a worker
            # completion (or a regenerate trigger) to silently destroy
            # a user upload.
            for v in list(node.variants):
                if getattr(v, 'source', 'ai') == 'manual':
                    continue
                db.delete(v)
            db.flush()

            # Create new variants — store paths relative to images_root()
            n_added = 0
            for idx, rel_or_abs in enumerate(output_paths, start=1):
                p = Path(rel_or_abs)
                if not p.is_absolute():
                    p = output_dir / p
                try:
                    rel = p.resolve().relative_to(images_root().resolve())
                except Exception:
                    # File lives outside images_root() — copy it in
                    dest = node_dir(node_id) / p.name
                    try:
                        shutil.copy2(p, dest)
                        rel = dest.relative_to(images_root())
                    except Exception as e:
                        log.error(f"[image_platform] Can't copy variant {p}: {e}")
                        continue
                v = ImageVariant(
                    node_id=node.id,
                    variant_index=idx,
                    image_path=str(rel).replace("\\", "/"),
                )
                db.add(v)
                n_added += 1

            if n_added == 0:
                node.status = "failed"
                node.error_message = "Worker reported completion but no variant files were usable"
            else:
                node.status = "ready"
                node.error_message = None
        else:
            node.status = "failed"
            node.error_message = error or "Unknown error"

        node.updated_at = datetime.utcnow()
        db.commit()
        log.info(f"[image_platform] Node {node_id} → {node.status}  variants={len(output_paths)}")

    _cleanup_job_files(node_id)


def _cleanup_job_files(node_id: int):
    watch = jobs_watch_dir()
    for fn in (f"node_{node_id}.json", f"node_{node_id}.done.json"):
        try:
            (watch / fn).unlink(missing_ok=True)
        except Exception:
            pass


# =============================================================================
# API Router
# =============================================================================

router = APIRouter(prefix="/api/images", tags=["images"])


def _max_parents() -> int:
    return 3


def _validate_parents(db: Session, parents: List[ParentRef]):
    if len(parents) > _max_parents():
        raise HTTPException(400, f"Max {_max_parents()} parents allowed")
    seen_slots = set()
    for p in parents:
        if p.slot_order in seen_slots:
            raise HTTPException(400, f"Duplicate slot_order {p.slot_order}")
        seen_slots.add(p.slot_order)
        node = db.query(ImageNode).filter(ImageNode.id == p.parent_node_id).first()
        if node is None:
            raise HTTPException(400, f"Parent node {p.parent_node_id} not found")


def _replace_parents(db: Session, child: ImageNode, parents: List[ParentRef]):
    # Delete existing edges
    for e in list(child.parent_edges):
        db.delete(e)
    db.flush()
    # Add new ones
    for p in parents:
        db.add(ImageEdge(
            parent_node_id=p.parent_node_id,
            child_node_id=child.id,
            role=p.role,
            slot_order=p.slot_order,
        ))


def _delete_variant_files(node: ImageNode):
    d = node_dir(node.id)
    for v in node.variants:
        try:
            # Delete R2 backup too (ignores errors)
            _storage_delete(v.image_path)
            p = images_root() / v.image_path
            if p.exists():
                p.unlink()
        except Exception as e:
            log.warning(f"Couldn't delete variant file {v.image_path}: {e}")
    # Clean any stragglers
    for f in d.glob("variant_*.png"):
        try:
            f.unlink()
        except Exception:
            pass


# ---- list / detail --------------------------------------------------------

@router.get("/nodes")
def list_nodes(
    request: Request,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    # v551 — eager-load every relationship that to_dict() touches.
    # The previous lazy-loading pattern was the root cause of the
    # "psycopg2.OperationalError: server closed the connection
    # unexpectedly" errors that fired during /nodes responses:
    #
    #   1. db.query(ImageNode)...all() succeeds → connection alive
    #   2. List comprehension iterates each node, calls to_dict()
    #   3. to_dict touches self.variants, self.child_edges, etc. —
    #      each access triggers a SEPARATE lazy SELECT round-trip
    #   4. Between two round-trips, the Postgres connection dies
    #      (Render network blip, NAT idle timeout, managed PG
    #      maintenance) → next lazy-load explodes
    #
    # With ~100 nodes and ~3 children each, the old code issued
    # ~700 round-trips per page load, each one a chance for the
    # connection to die. Eager loading collapses that to 4-5
    # round-trips (one per relationship) regardless of N. Even
    # if a connection dies, the window where it can hit is shrunk
    # by ~99%.
    #
    # selectinload is the right strategy for collections (issues
    # one extra IN-query per relationship); joinedload is right
    # for single-object FKs (joins inline). The chain follows
    # what to_dict() actually accesses:
    #   - variants (collection)
    #   - parent_edges → parent (FK on each edge)
    #   - child_edges → child (FK on each edge, only status read)
    #
    # v551 — also wrap in read_query_with_retry. Even with eager
    # loading reducing the lazy-load surface to zero, the initial
    # query itself can still land on a dead pool connection that
    # pre_ping didn't catch (mid-second connection death). One
    # retry on dead-connection errors is the documented production
    # SQLAlchemy pattern for read paths.
    nodes = read_query_with_retry(db, lambda: db.query(ImageNode).filter(
        ImageNode.user_id == current_user.id
    ).options(
        selectinload(ImageNode.variants),
        selectinload(ImageNode.parent_edges).joinedload(ImageEdge.parent),
        selectinload(ImageNode.child_edges).joinedload(ImageEdge.child),
    ).order_by(ImageNode.created_at.desc()).all())

    # v640 — ETag/304 support to kill bandwidth waste from 2s sidebar polling.
    # User HAR capture showed `/api/images/nodes` returning 2.9 MB every 2 s
    # (~21 MB/min sustained) even when nothing was changing. Browser caches
    # don't help because the route uses POST-style auth (cookie). ETag lets
    # the server skip the body when the response would be byte-identical.
    #
    # MD5 of the serialized payload as the cache key. Hashing 2.9 MB ≈ 5-15 ms,
    # negligible vs the alternative of shipping 2.9 MB every poll. When the
    # user is mid-edit (something queued/generating), the body changes, hash
    # mismatches, and the full response is sent. When idle, browser sends
    # If-None-Match and gets a 49-byte 304 instead.
    import hashlib
    import json as _json
    from fastapi.responses import Response as _FAResponse
    payload = {"nodes": [n.to_dict(include_variants=True) for n in nodes]}
    body = _json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    etag = '"' + hashlib.md5(body).hexdigest() + '"'
    headers = {
        "ETag": etag,
        "Cache-Control": "private, max-age=0, must-revalidate",
    }
    if request.headers.get("if-none-match") == etag:
        return _FAResponse(status_code=304, headers=headers)
    return _FAResponse(content=body, media_type="application/json", headers=headers)


@router.get("/nodes/{node_id}")
def get_node(
    node_id: int,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    # v551 — same eager-loading + retry strategy as /nodes.
    node = read_query_with_retry(db, lambda: db.query(ImageNode).filter(
        ImageNode.id == node_id,
        ImageNode.user_id == current_user.id,
    ).options(
        selectinload(ImageNode.variants),
        selectinload(ImageNode.parent_edges).joinedload(ImageEdge.parent),
        selectinload(ImageNode.child_edges).joinedload(ImageEdge.child),
    ).first())
    if not node:
        raise HTTPException(404, "Node not found")
    return node.to_dict(include_variants=True)


@router.get("/graph")
def get_graph(
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Return nodes + edges for graph-view rendering. User-scoped — only
    this user's nodes and the edges between them."""
    nodes = db.query(ImageNode).filter(ImageNode.user_id == current_user.id).all()
    node_ids = {n.id for n in nodes}
    # Edges: both endpoints must be in this user's node set
    all_edges = db.query(ImageEdge).all()
    edges = [e for e in all_edges
             if e.parent_node_id in node_ids and e.child_node_id in node_ids]
    return {
        "nodes": [
            {
                "id": n.id,
                "name": n.name or (f"Node {n.id}" if n.kind == "generated" else f"Upload {n.id}"),
                "kind": n.kind,
                "status": n.status,
                "chosen_variant_url": (
                    f"/api/images/files/{next((v.image_path for v in n.variants if v.id == n.chosen_variant_id), '')}"
                    if n.chosen_variant_id else None
                ),
            }
            for n in nodes
        ],
        "edges": [e.to_dict() for e in edges],
    }


# ---- create / update / delete --------------------------------------------

@router.post("/nodes")
def create_node(
    req: CreateNodeRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    # Validate parents exist AND belong to the current user — can't reference
    # another user's nodes as your parents.
    _validate_parents(db, req.parents)
    for p in req.parents:
        parent = db.query(ImageNode).filter(ImageNode.id == p.parent_node_id).first()
        if parent and parent.user_id and parent.user_id != current_user.id:
            raise HTTPException(403, f"Parent node {p.parent_node_id} belongs to another user")

    node = ImageNode(
        user_id=current_user.id,
        name=req.name,
        kind="generated",
        prompt=req.prompt,
        aspect_ratio=req.aspect_ratio,
        resolution=req.resolution,
        model=req.model,
        n_variants=req.n_variants,
        status="draft",
    )
    db.add(node)
    db.flush()  # get node.id
    for p in req.parents:
        db.add(ImageEdge(
            parent_node_id=p.parent_node_id,
            child_node_id=node.id,
            role=p.role,
            slot_order=p.slot_order,
        ))
    db.commit()
    db.refresh(node)
    return node.to_dict()


@router.patch("/nodes/{node_id}")
def update_node(
    node_id: int,
    req: UpdateNodeRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    node = db.query(ImageNode).filter(
        ImageNode.id == node_id,
        ImageNode.user_id == current_user.id,
    ).first()
    if not node:
        raise HTTPException(404, "Node not found")
    if node.kind == "upload":
        raise HTTPException(400, "Upload nodes cannot be edited")
    if node.status in ("queued", "generating"):
        raise HTTPException(409, f"Cannot edit while {node.status}")

    for field in ("name", "prompt", "aspect_ratio", "resolution", "model", "n_variants"):
        v = getattr(req, field)
        if v is not None:
            setattr(node, field, v)

    if req.parents is not None:
        _validate_parents(db, req.parents)
        _replace_parents(db, node, req.parents)

    node.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(node)
    return node.to_dict()


@router.delete("/nodes/{node_id}")
def delete_node(
    node_id: int,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    node = db.query(ImageNode).filter(
        ImageNode.id == node_id,
        ImageNode.user_id == current_user.id,
    ).first()
    if not node:
        raise HTTPException(404, "Node not found")
    # Check no children depend on this node
    n_children = db.query(ImageEdge).filter(ImageEdge.parent_node_id == node_id).count()
    if n_children > 0:
        # Get child node names for a helpful message
        child_ids = [
            e.child_node_id for e in
            db.query(ImageEdge).filter(ImageEdge.parent_node_id == node_id).limit(5).all()
        ]
        child_names = []
        for cid in child_ids:
            cn = db.query(ImageNode).filter(ImageNode.id == cid).first()
            if cn:
                child_names.append(cn.name or f"#{cid}")
        names_str = ", ".join(child_names)
        if n_children > 5:
            names_str += f" (and {n_children - 5} more)"
        raise HTTPException(
            409,
            f"Cannot delete — {n_children} scene(s) use this as a parent: {names_str}. "
            f"Delete those first."
        )

    try:
        # ===== Scene-assignment merge =====
        # If this node is referenced by one or more ImageSceneAssignment rows
        # (v432+ batches), the assignments' voiceover lines and action_notes
        # would be lost when we delete the node. Preserve them by merging
        # each assignment's lines into an adjacent scene assignment before
        # destroying it. Prefer the previous scene (lower scene_index); fall
        # back to the next scene if this is the first.
        #
        # Handles image reuse: when the same image_node_id is referenced
        # by multiple scenes (new-format md where scenes reuse images), we
        # merge each one individually. Processing in scene_index order
        # ensures deterministic results when neighbors themselves get
        # collapsed mid-loop.
        #
        # If the only scenes in the batch are ones referencing this node,
        # the line in the very first scene is lost (nowhere to merge into).
        # In that pathological case we log a warning.
        import json as _json
        own_assignments = db.query(ImageSceneAssignment).filter(
            ImageSceneAssignment.image_node_id == node_id
        ).order_by(ImageSceneAssignment.scene_index.asc()).all()

        batches_touched = set()
        for own_assignment in own_assignments:
            batch_id_for_merge = own_assignment.batch_id
            batches_touched.add(batch_id_for_merge)

            # Find a merge target — any non-orphan assignment in the same
            # batch that isn't scheduled for deletion in this same delete
            # call. "Scheduled for deletion" = any assignment pointing at
            # this node (since we're deleting the node). We exclude those
            # from the candidate set so we don't try to merge into a row
            # that's about to vanish.
            to_delete_ids = {a.id for a in own_assignments}

            # Previous scene (lower scene_index, same batch, not in to_delete)
            prev_assignment = db.query(ImageSceneAssignment).filter(
                ImageSceneAssignment.batch_id == batch_id_for_merge,
                ImageSceneAssignment.scene_index < own_assignment.scene_index,
                ~ImageSceneAssignment.id.in_(to_delete_ids),
            ).order_by(ImageSceneAssignment.scene_index.desc()).first()

            merge_target = prev_assignment
            if merge_target is None:
                # No previous candidate — fall back to next scene
                merge_target = db.query(ImageSceneAssignment).filter(
                    ImageSceneAssignment.batch_id == batch_id_for_merge,
                    ImageSceneAssignment.scene_index > own_assignment.scene_index,
                    ~ImageSceneAssignment.id.in_(to_delete_ids),
                ).order_by(ImageSceneAssignment.scene_index.asc()).first()

            if merge_target is not None:
                # Parse existing lines + action_notes from both, append ours
                try:
                    target_lines = _json.loads(merge_target.lines_json or "[]")
                except Exception:
                    target_lines = []
                try:
                    target_notes = _json.loads(merge_target.action_notes_json or "null")
                    if target_notes is None:
                        target_notes = [None] * len(target_lines)
                except Exception:
                    target_notes = [None] * len(target_lines)

                try:
                    own_lines = _json.loads(own_assignment.lines_json or "[]")
                except Exception:
                    own_lines = []
                try:
                    own_notes = _json.loads(own_assignment.action_notes_json or "null")
                    if own_notes is None:
                        own_notes = [None] * len(own_lines)
                except Exception:
                    own_notes = [None] * len(own_lines)

                # Normalize action_notes length to match lines length on both
                while len(target_notes) < len(target_lines):
                    target_notes.append(None)
                target_notes = target_notes[:len(target_lines)]
                while len(own_notes) < len(own_lines):
                    own_notes.append(None)
                own_notes = own_notes[:len(own_lines)]

                merged_lines = target_lines + own_lines
                merged_notes = target_notes + own_notes

                merge_target.lines_json = _json.dumps(merged_lines)
                merge_target.action_notes_json = _json.dumps(merged_notes)
                log.info(
                    f"[delete_node {node_id}] merged {len(own_lines)} line(s) "
                    f"from scene {own_assignment.scene_index} → scene "
                    f"{merge_target.scene_index} (batch {batch_id_for_merge})"
                )
            else:
                # No merge target exists anywhere in this batch — pathological
                # case where every scene in the batch referenced this one
                # deleted node. Lines are lost.
                try:
                    lost = _json.loads(own_assignment.lines_json or "[]")
                except Exception:
                    lost = []
                log.warning(
                    f"[delete_node {node_id}] no merge target in batch "
                    f"{batch_id_for_merge}; {len(lost)} line(s) will be lost"
                )

            db.delete(own_assignment)
        if own_assignments:
            db.flush()

        # Re-sequence scene_index on remaining assignments in every touched
        # batch so they stay contiguous starting at 0. Keeps the prepare
        # endpoint's output predictable and the video tab's scene numbering
        # sane after deletions.
        for batch_id_touched in batches_touched:
            remaining = db.query(ImageSceneAssignment).filter(
                ImageSceneAssignment.batch_id == batch_id_touched
            ).order_by(ImageSceneAssignment.scene_index).all()
            for new_idx, a in enumerate(remaining):
                if a.scene_index != new_idx:
                    a.scene_index = new_idx
            db.flush()

        # Break the FK reference to chosen_variant_id so variants can be deleted
        node.chosen_variant_id = None
        db.flush()

        # Delete files (local + R2)
        _delete_variant_files(node)

        # Remove the node's directory if it still exists
        try:
            d = node_dir(node_id)
            if d.exists():
                shutil.rmtree(d, ignore_errors=True)
        except Exception as e:
            log.warning(f"[image_platform] Couldn't remove node dir: {e}")

        db.delete(node)
        db.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        log.exception(f"[image_platform] delete_node({node_id}) failed: {e}")
        db.rollback()
        raise HTTPException(500, f"Delete failed: {e}")


@router.delete("/batches/{batch_id}")
def delete_batch(
    batch_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Delete an entire batch: its nodes (leaves first so FK checks pass),
    its scene assignments, and the batch row itself.

    Why leaves first: each scene node may have child scenes that reference
    it via ImageEdge (parent/child). deleting a parent before its children
    raises 409 from the per-node delete logic. Iterating in descending
    scene_index order ensures every node is a leaf by the time we try to
    delete it.

    Uploaded subject nodes (kind='upload') are NOT deleted — they're
    outside the batch (no batch_id) and may be reused for future batches.
    """
    batch = db.query(ImageJobBatch).filter(
        ImageJobBatch.id == batch_id,
        ImageJobBatch.user_id == current_user.id,
    ).first()
    if not batch:
        raise HTTPException(404, f"Batch {batch_id} not found")

    # Gather all nodes in this batch, sorted so leaves (highest scene_index)
    # come first — their parents haven't been deleted yet so they're clean
    # to remove. nodes with NULL scene_index_in_batch sort to the end of
    # the ASC sort; flipping to DESC puts them first, which is actually
    # fine for deletion order since they have no known ordering anyway.
    nodes = db.query(ImageNode).filter(
        ImageNode.batch_id == batch_id,
        ImageNode.user_id == current_user.id,
    ).order_by(ImageNode.scene_index_in_batch.desc().nullsfirst()).all()

    deleted_count = 0
    failed_ids = []

    for node in nodes:
        node_id = node.id
        try:
            # Break chosen_variant_id FK so variants can be deleted
            node.chosen_variant_id = None
            db.flush()

            # Delete associated scene assignments (if any) — these have an
            # FK to image_node_id but no CASCADE, so we clear them first.
            db.query(ImageSceneAssignment).filter(
                ImageSceneAssignment.image_node_id == node_id
            ).delete(synchronize_session=False)

            # Delete variant files (local + R2)
            _delete_variant_files(node)

            # Remove the node's on-disk dir if it exists
            try:
                d = node_dir(node_id)
                if d.exists():
                    shutil.rmtree(d, ignore_errors=True)
            except Exception as e:
                log.warning(f"[image_platform] Couldn't remove node dir for {node_id}: {e}")

            db.delete(node)
            db.flush()
            deleted_count += 1
        except Exception as e:
            log.warning(f"[image_platform] delete_batch: couldn't delete node {node_id}: {e}")
            failed_ids.append(node_id)
            # Don't rollback — continue with other nodes. Partial delete
            # is better than all-or-nothing because the user's intent was
            # "remove this batch".
            db.rollback()

    # Clean up any leftover scene assignments tied to this batch (in case
    # some weren't caught by the per-node loop above)
    try:
        db.query(ImageSceneAssignment).filter(
            ImageSceneAssignment.batch_id == batch_id
        ).delete(synchronize_session=False)
    except Exception as e:
        log.warning(f"[image_platform] delete_batch: scene assignment cleanup failed: {e}")

    # Finally delete the batch row itself. If some nodes failed to delete,
    # the batch row is still orphaned — better to remove it so the user
    # can retry a clean import under the same name.
    try:
        db.delete(batch)
        db.commit()
    except Exception as e:
        log.exception(f"[image_platform] delete_batch: couldn't delete batch row {batch_id}: {e}")
        db.rollback()
        raise HTTPException(500, f"Batch deletion partial (deleted {deleted_count} node(s)) but batch row cleanup failed: {e}")

    return {
        "ok": True,
        "deleted_nodes": deleted_count,
        "failed_nodes": failed_ids,
        "batch_id": batch_id,
    }


# ---- generate / regenerate / choose --------------------------------------

@router.post("/nodes/{node_id}/generate")
def generate_node(
    node_id: int,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    node = db.query(ImageNode).filter(
        ImageNode.id == node_id,
        ImageNode.user_id == current_user.id,
    ).first()
    if not node:
        raise HTTPException(404, "Node not found")
    if node.kind == "upload":
        raise HTTPException(400, "Upload nodes can't be generated")
    if node.status in ("queued", "generating"):
        raise HTTPException(409, f"Already {node.status}")

    # Validate parents are ready (surfaces a nice error early) BEFORE mutating state
    _resolve_parent_image_paths(db, node)

    # Clean any existing variants (shouldn't be any for a draft, but just in case)
    _delete_variant_files(node)
    for v in list(node.variants):
        db.delete(v)
    node.chosen_variant_id = None
    node.error_message = None
    node.status = "queued"
    db.flush()

    try:
        write_generation_job(db, node)
    except Exception as e:
        # Roll back so the node isn't stranded in 'queued' with no job file
        node.status = "failed"
        node.error_message = f"Failed to queue job: {e}"
        db.commit()
        raise HTTPException(500, f"Failed to queue job: {e}")

    db.commit()
    db.refresh(node)
    return node.to_dict()


@router.post("/nodes/{node_id}/regenerate")
def regenerate_node(
    node_id: int,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    node = db.query(ImageNode).filter(
        ImageNode.id == node_id,
        ImageNode.user_id == current_user.id,
    ).first()
    if not node:
        raise HTTPException(404, "Node not found")
    if node.kind == "upload":
        raise HTTPException(400, "Upload nodes can't be regenerated")
    if node.status in ("queued", "generating"):
        raise HTTPException(409, f"Already {node.status}")

    _resolve_parent_image_paths(db, node)

    _delete_variant_files(node)
    for v in list(node.variants):
        db.delete(v)
    node.chosen_variant_id = None
    node.error_message = None
    node.status = "queued"
    db.flush()

    try:
        write_generation_job(db, node)
    except Exception as e:
        node.status = "failed"
        node.error_message = f"Failed to queue job: {e}"
        db.commit()
        raise HTTPException(500, f"Failed to queue job: {e}")

    db.commit()
    db.refresh(node)
    return node.to_dict()


@router.post("/nodes/{node_id}/abort")
def abort_node(
    node_id: int,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Force-release a node that's stuck in 'generating' status.

    Normal flow: worker claims a node (status → generating), runs it,
    POSTs completion. The node transitions back to 'ready' or 'failed'.

    Stuck flow: the worker dies mid-job. The node stays 'generating'
    until the stale-claim sweep inside `/jobs/pending` runs (10-min TTL)
    AND a worker polls. If the worker stays offline, the node is stuck
    forever with no UI path to reset it.

    This endpoint is the escape hatch: set status back to 'queued' (so
    the next worker will pick it up) and clear the claim fields. Also
    clears error_message in case a previous run set one.

    Safe to call when status is 'generating' OR 'queued' (idempotent).
    Rejects other statuses so the user can't accidentally wipe a ready
    node's claim info.
    """
    node = db.query(ImageNode).filter(
        ImageNode.id == node_id,
        ImageNode.user_id == current_user.id,
    ).first()
    if not node:
        raise HTTPException(404, "Node not found")
    if node.status not in ("generating", "queued"):
        raise HTTPException(
            409,
            f"Can only abort nodes in 'generating' or 'queued' status "
            f"(this node is '{node.status}')"
        )

    prev_worker = node.claimed_by_worker
    node.status = "queued"
    node.claimed_by_worker = None
    node.claimed_at = None
    node.error_message = None
    db.commit()
    db.refresh(node)
    log.info(
        f"[image_platform] Node {node_id} aborted "
        f"(was claimed by {prev_worker or 'none'}, now re-queued)"
    )
    return node.to_dict()


@router.post("/nodes/{node_id}/choose")
def choose_variant(
    node_id: int,
    req: ChooseVariantRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    node = db.query(ImageNode).filter(
        ImageNode.id == node_id,
        ImageNode.user_id == current_user.id,
    ).first()
    if not node:
        raise HTTPException(404, "Node not found")
    variant = db.query(ImageVariant).filter(
        ImageVariant.id == req.variant_id,
        ImageVariant.node_id == node_id,
    ).first()
    if not variant:
        raise HTTPException(404, "Variant not found on this node")
    node.chosen_variant_id = variant.id
    if node.status != "ready":
        node.status = "ready"
    node.updated_at = datetime.utcnow()
    db.commit()

    # Auto-promote any draft children that were waiting on this node
    try:
        _promote_ready_children(db, node.id)
    except Exception as e:
        log.warning(f"[image_platform] Child promotion after choose failed: {e}")

    db.refresh(node)
    return node.to_dict()


# ---- uploads (seed nodes) ------------------------------------------------

@router.post("/uploads")
async def upload_reference(
    file: UploadFile = File(...),
    name: Optional[str] = Form(None),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Upload a reference image. Creates a 'seed node' (kind=upload,
    status=ready, one variant = the uploaded file)."""
    # Validate type
    ext = Path(file.filename or "").suffix.lower() or ".png"
    if ext not in (".png", ".jpg", ".jpeg", ".webp"):
        raise HTTPException(400, "Only png/jpg/webp allowed")

    # Save file locally
    filename = f"{uuid.uuid4().hex}{ext}"
    abs_path = uploads_root() / filename
    content = await file.read()
    abs_path.write_bytes(content)

    rel = abs_path.relative_to(images_root())
    rel_str = str(rel).replace("\\", "/")

    # Mirror to R2 so the file survives ephemeral-filesystem redeploys
    _storage_upload_file(abs_path, rel_str)

    # Create seed node — stamp owner
    display_name = name or (Path(file.filename).stem if file.filename else f"upload_{filename[:8]}")
    node = ImageNode(
        user_id=current_user.id,
        name=display_name,
        kind="upload",
        prompt=None,
        status="ready",
        n_variants=1,
    )
    db.add(node)
    db.flush()
    variant = ImageVariant(
        node_id=node.id,
        variant_index=1,
        image_path=rel_str,
    )
    db.add(variant)
    db.flush()
    node.chosen_variant_id = variant.id
    db.commit()
    db.refresh(node)
    return node.to_dict()


# ---- manual variant upload (v530) ----------------------------------------

@router.post("/nodes/{node_id}/manual-variant")
async def upload_manual_variant(
    node_id: int,
    file: UploadFile = File(...),
    mode: str = Query("add", regex="^(add|replace)$"),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Upload a user-supplied image into a specific node's variant pool.

    The uploaded file becomes a peer to AI-generated variants:
      - Inserted as ImageVariant with source='manual'
      - Auto-selected (set as the node's chosen_variant_id)
      - Renders alongside AI variants in the variant grid (UI distinguishes
        with an 'M' badge / folder icon)
      - Used as a reference parent for downstream nodes whenever it's
        selected at their generation time

    `mode=add` (default): always inserts a new variant.
    `mode=replace`: deletes any existing manual variants for this node
                    first, then inserts the new one. Existing AI variants
                    are NOT touched (replace only affects manual peers).

    Aspect ratio: warns if the upload isn't 9:16 (within ±2%) but always
    accepts. Veo / Nano Banana 2 will downsample/letterbox at use time.

    Auto-selection: regardless of mode, the new variant becomes the node's
    chosen_variant_id and the node is promoted to status='ready'. Any
    draft children waiting on this node get promoted via the existing
    _promote_ready_children helper.
    """
    from PIL import Image as PILImage  # local import — only needed here
    import io as _io

    # --- Validate ownership + node ---
    node = db.query(ImageNode).filter(
        ImageNode.id == node_id,
        ImageNode.user_id == current_user.id,
    ).first()
    if not node:
        raise HTTPException(404, "Node not found")

    # --- Validate file type by extension ---
    raw_filename = file.filename or "upload.png"
    ext = Path(raw_filename).suffix.lower() or ".png"
    if ext not in (".png", ".jpg", ".jpeg", ".webp"):
        raise HTTPException(400, "Only png/jpg/webp allowed")

    # --- Read bytes (cap at 10 MB) ---
    MAX_BYTES = 10 * 1024 * 1024
    content = await file.read()
    if not content:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_BYTES:
        raise HTTPException(413, f"File too large (>{MAX_BYTES // (1024*1024)} MB)")

    # --- Validate decodable + capture dimensions ---
    try:
        with PILImage.open(_io.BytesIO(content)) as _probe:
            _probe.verify()  # raises if corrupt
    except Exception as e:
        raise HTTPException(400, f"Not a valid image: {e}")
    # Re-open for dimension read (verify() consumed the stream)
    try:
        with PILImage.open(_io.BytesIO(content)) as _img:
            width, height = _img.size
    except Exception as e:
        raise HTTPException(400, f"Could not read image dimensions: {e}")

    if width <= 0 or height <= 0:
        raise HTTPException(400, "Image has no dimensions")

    # --- Aspect-ratio warning (9:16 = 0.5625, ±2% tolerance) ---
    ratio = width / height
    target = 9.0 / 16.0
    tolerance = 0.02
    aspect_warning = None
    if abs(ratio - target) / target > tolerance:
        aspect_warning = (
            f"Expected 9:16 (~0.563), got {width}x{height} ({ratio:.3f}). "
            f"Downstream generation may crop or letterbox."
        )

    # --- Replace mode: hard-delete existing manual variants for this node ---
    # AI variants are untouched. This handles the user's "ask each time"
    # choice — the UI presents the modal, then sends mode=replace if the
    # user picked overwrite.
    if mode == "replace":
        existing_manual = db.query(ImageVariant).filter(
            ImageVariant.node_id == node_id,
            ImageVariant.source == 'manual',
        ).all()
        for v in existing_manual:
            # Break chosen_variant_id FK if it points at this row
            if node.chosen_variant_id == v.id:
                node.chosen_variant_id = None
                db.flush()
            # Delete the local file too — the variant is gone
            try:
                abs_p = images_root() / v.image_path
                if abs_p.exists():
                    abs_p.unlink()
            except Exception as _e:
                log.warning(f"[manual-variant] could not delete {v.image_path}: {_e}")
            db.delete(v)
        db.flush()

    # --- Compute next variant_index (sort after existing variants) ---
    max_idx_q = db.query(ImageVariant.variant_index).filter(
        ImageVariant.node_id == node_id,
    ).all()
    existing_indices = [r[0] for r in max_idx_q if r[0] is not None]
    next_index = (max(existing_indices) + 1) if existing_indices else 1

    # --- Save file to node directory (mirrors AI-variant layout) ---
    # node_dir(N) returns _data_root()/images/node_N — same place AI
    # variants are written. Filename uses a UUID so multiple manual
    # uploads can coexist without collision.
    target_dir = node_dir(node_id)
    filename = f"manual_{uuid.uuid4().hex}{ext}"
    abs_path = target_dir / filename
    abs_path.write_bytes(content)

    rel = abs_path.relative_to(images_root())
    rel_str = str(rel).replace("\\", "/")

    # --- Mirror to R2 so the file survives ephemeral-filesystem redeploys ---
    _storage_upload_file(abs_path, rel_str)

    # --- Create the variant row ---
    variant = ImageVariant(
        node_id=node_id,
        variant_index=next_index,
        image_path=rel_str,
        source='manual',
    )
    db.add(variant)
    db.flush()

    # --- Auto-select (Q3 answer) ---
    node.chosen_variant_id = variant.id
    if node.status != "ready":
        node.status = "ready"
    node.updated_at = datetime.utcnow()
    db.commit()

    # --- Promote any draft children waiting on this node (mirrors choose_variant) ---
    try:
        _promote_ready_children(db, node.id)
    except Exception as e:
        log.warning(f"[manual-variant] Child promotion failed: {e}")

    db.refresh(variant)

    return {
        **variant.to_dict(),
        "dimensions": {"width": width, "height": height, "ratio": round(ratio, 4)},
        "aspect_warning": aspect_warning,
        "selected": True,
        "node_status": "ready",
    }


# ---- file serving --------------------------------------------------------

@router.get("/files/{path:path}")
def serve_image_file(
    path: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Serve an image by its path relative to images_root().
    Falls back to R2 (if configured) when the local file is missing.

    Ownership: the path must correspond to a variant whose ImageNode's
    user_id matches current_user.id. Variants not associated with any
    node (legacy data, truly orphaned) are served without the ownership
    check since they have no owner to compare against.

    v527: previously held the dep-injected DB session through R2
    fallback fetches AND through FileResponse streaming. Under heavy
    frontend polling (each user views N variant tiles, frontend polls
    on focus/refresh) that exhausted the 90-connection pool. Worker
    process couldn't get a connection within 5s pool_timeout and
    crashed. Now: query needed data, release session BEFORE R2 work
    or FileResponse, reopen briefly for any DB writes (orphan cleanup).

    Sets Cache-Control: no-cache so browsers always revalidate."""
    # Prevent path traversal
    safe = Path(path).as_posix()
    if ".." in safe.split("/"):
        raise HTTPException(400, "Invalid path")
    abs_path = (images_root() / safe).resolve()
    try:
        abs_path.relative_to(images_root().resolve())
    except ValueError:
        raise HTTPException(400, "Invalid path")

    # Ownership check — capture variant ID + node user_id, then RELEASE
    # the session before any slow work.
    variant = db.query(ImageVariant).filter(ImageVariant.image_path == safe).first()
    variant_id = variant.id if variant else None
    if variant is not None:
        node = db.query(ImageNode).filter(ImageNode.id == variant.node_id).first()
        if node is not None and node.user_id and node.user_id != current_user.id:
            # Don't reveal existence of another user's file — return 404
            raise HTTPException(404, "File not found")

    # v527: release the dep-injected session before slow I/O. The dep
    # wrapper's finally block will call db.close() again (no-op).
    db.close()

    if not abs_path.exists():
        # v561: redirect-to-R2 fast path for cold cache.
        #
        # Pre-v561 behavior was: download the file synchronously from R2
        # (~200ms-2s per file), then serve via FileResponse. That made
        # post-deploy startup catastrophic — Render wipes /app/data/, the
        # frontend immediately renders dozens of variant <img> tags, each
        # triggering a blocking R2 download here, the eventloop starves
        # under the load, health checks fail, Render SIGTERMs the worker
        # before the deferred startup even finishes (observed: 63 seconds
        # from "Application startup complete" to SIGTERM with hundreds of
        # downloads still in flight).
        #
        # v561: when a variant is missing locally and IS in R2, return
        # a 302 redirect to a presigned R2 URL. The browser fetches bytes
        # directly from R2's edge — typically 50-100ms — and the app
        # worker spends ~5ms on this request instead of 500-2000ms.
        # Browser caches it via the presigned URL.
        #
        # We still kick off a background fetch to cache locally for the
        # next request that needs the actual local file (worker reads
        # for video pipelines, /worker/files/{token} downloads, etc.).
        # That async backfill doesn't block this response.
        from fastapi.responses import RedirectResponse
        storage = _storage_or_none()
        if storage is not None and safe not in _r2_known_missing:
            try:
                key = _r2_key_for(safe)
                # HEAD first — cheap, confirms object exists, lets us
                # distinguish "not in R2" from "in R2 but transient error"
                storage.client.head_object(Bucket=storage.bucket_name, Key=key)
                # Object exists; generate a 1-hour presigned URL and redirect.
                presigned = storage.get_presigned_url(key, expires_in=3600)
                # Schedule async backfill so subsequent requests hit local.
                # Fire-and-forget — never blocks the redirect.
                try:
                    import threading
                    def _bg_download():
                        try:
                            _storage_download_to_local(safe)
                        except Exception:
                            pass
                    threading.Thread(target=_bg_download, daemon=True).start()
                except Exception:
                    pass
                # 302 with Cache-Control to let the browser cache the
                # bytes for the duration of the presigned URL.
                return RedirectResponse(
                    url=presigned,
                    status_code=302,
                    headers={"Cache-Control": "private, max-age=3600"},
                )
            except Exception as e:
                # HEAD failed — either the object truly isn't in R2 (404)
                # or there's a real R2 error. Fall through to the legacy
                # path (synchronous download attempt + orphan cleanup).
                # Mark known-missing in the 404 case to short-circuit
                # future requests for the same path.
                msg = str(e)
                if "Not Found" in msg or "NoSuchKey" in msg or "(404)" in msg:
                    _r2_known_missing.add(safe)

        # v561: fall-through legacy path — used when storage isn't
        # configured, when HEAD failed for non-404 reasons (the next
        # request retries), or when the file truly isn't in R2 (orphan
        # cleanup runs).
        restored = _storage_download_to_local(safe)
        if not restored or not abs_path.exists():
            # v523.2 / v527: lazy orphan cleanup, with a fresh brief
            # session (not the dep-injected one which is closed).
            if variant_id is not None:
                from models import get_db
                try:
                    with get_db() as db2:
                        v_now = db2.query(ImageVariant).filter(
                            ImageVariant.id == variant_id
                        ).first()
                        if v_now is not None:
                            log.info(
                                f"[image_platform] Lazy orphan cleanup: variant {v_now.id} "
                                f"({safe}) missing locally and not in R2 → removing DB row"
                            )
                            if v_now.node_id:
                                owner = db2.query(ImageNode).filter(
                                    ImageNode.id == v_now.node_id
                                ).first()
                                if owner and owner.chosen_variant_id == v_now.id:
                                    owner.chosen_variant_id = None
                            db2.delete(v_now)
                            db2.commit()
                except Exception as e:
                    log.warning(f"[image_platform] Lazy orphan cleanup failed: {e}")
            raise HTTPException(404, "File not found (and not in R2 backup)")

    return FileResponse(
        abs_path,
        headers={"Cache-Control": "no-cache"},
    )


@router.post("/cleanup-orphans")
def cleanup_orphans_endpoint(
    current_user: User = Depends(get_current_user),
):
    """Trigger a manual cleanup of orphan variants/nodes (files missing
    from disk and not restorable from R2). Useful after a fresh redeploy
    wipes local storage.

    Auth-gated but not user-scoped — the underlying cleanup is a global
    DB maintenance operation (only removes rows whose files are truly
    gone). Safe for any authenticated user to run.
    """
    result = cleanup_orphan_nodes()
    return result


# =============================================================================
# Scene table importer
# =============================================================================
# Parses a markdown file (see vinegar_video_complete.md) that describes a
# sequence of image-generation scenes with dependencies between them.
#
# Format:
#   ### Scene N
#   - **reference_image:** image_X  (or "none")
#   - ...other fields...
#   **Image prompt:**
#   ```
#   <full prompt text>
#   ```
#
# Dependencies:
#   - Each scene gets the user-chosen subject upload as parent (slot 0)
#   - If reference_image: image_X, that scene gets Scene X as parent (slot 1)
#
# Scheduling:
#   - Scenes whose parents are all ready+chosen start in status="queued"
#   - Scenes waiting on a parent start in status="draft" and auto-promote
#     to "queued" when the parent becomes ready (via _promote_ready_children)

import re as _re


def _parse_bullet_field(block: str, key: str) -> Optional[str]:
    """Extract a single bullet-field value from a scene block.

    Matches lines like:
        - **text:** Some value that may span to end of line.
        - **rhythm tier:** authority (17w)

    The key is matched case-insensitively. Value captures everything
    after the colon up to the newline. Returns the trimmed string, or
    None if not present.

    If the field has a parenthetical description after the value
    (e.g. "none (register-change cut from hook; ...)"), we keep
    the primary token and strip the paren suffix for structured
    fields. Callers that want the full raw value can parse freely.
    """
    # Allow the key to use either space or underscore (md uses both
    # styles — "rhythm tier" vs "scene_transition"). Escape the key
    # for regex but let spaces match either space or underscore.
    key_pattern = _re.escape(key).replace(r"\ ", r"[\s_]")
    pat = rf"^\s*-\s*\*\*{key_pattern}\s*:\*\*\s*(.+?)\s*$"
    m = _re.search(pat, block, flags=_re.MULTILINE | _re.IGNORECASE)
    if not m:
        return None
    return m.group(1).strip()


def _normalize_speaker_mode(raw: Optional[str]) -> Optional[str]:
    """v537 — canonicalize the writer's `**speaker:**` value.

    The markdown convention accepts a few synonymous spellings so writers
    don't have to remember the exact tokens. We collapse them to the four
    canonical values that the prompt-builder branches on:

        'on-camera' — visible main character speaks (lip-sync ON)
        'voiceover' — off-screen narrator (lip-sync OFF; deprecated for
                      v681 generate-side, deferred to v682; still
                      tolerated on read for legacy artifacts)
        'silent'    — no dialogue this scene (music / SFX only); no
                      `- **line:**` bullet required (v681)
        'auto'      — run _detect_voiceover_only (default if field absent)

    Accepted spellings (case-insensitive, ignoring spaces/dashes/underscores):
      on-camera   ←  on-camera | on camera | oncamera | on_camera | dialogue |
                     speaks    | spoken    | lip-sync | character |
                     character speaks
      voiceover   ←  voiceover | voice-over | voice over | vo | narration |
                     off-screen | offscreen | off screen | narrator |
                     narrated
      silent      ←  silent    | mute      | nodialogue| nospeech  |
                     (v681)       (v681)      (v681)      (v681)
                     music      | sfx       | broll     | b-roll    |
                     (musiconly)  (sfx-only)  (b-roll)    (b-roll)
      auto        ←  auto | detect | default | "" (empty)

    v681 — the speaker bullet may include a character name prefix:
        `the healer on-camera`  →  the LAST whitespace-separated token
                                   is the mode; preceding tokens are the
                                   character name (handled separately by
                                   the cast: bullet, not parsed here).
        `silent`                →  no character name; whole value is mode.
    The normalizer extracts the mode by checking the trailing token after
    the character-name strip.

    Anything unrecognized is returned as-is (lower-cased + trimmed) so we
    can surface the raw value in error messages without silently dropping
    it. Returns None when the field is entirely absent.
    """
    if raw is None:
        return None
    s = raw.strip().lower()
    if not s:
        return None

    def _flat(token: str) -> str:
        return "".join(ch for ch in token if ch.isalpha())

    def _classify(flat: str) -> Optional[str]:
        if flat in ("oncamera", "dialogue", "speaks", "spoken", "lipsync",
                    "character", "characterspeaks"):
            return "on-camera"
        if flat in ("voiceover", "vo", "narration", "offscreen", "narrator",
                    "narrated"):
            return "voiceover"
        if flat in ("silent", "mute", "nodialogue", "nospeech",
                    "music", "musiconly", "sfx", "sfxonly",
                    "broll", "brolloverlay"):
            return "silent"
        if flat in ("auto", "detect", "default"):
            return "auto"
        return None

    # First try the entire string as a single token (legacy v537 path).
    full_flat = _flat(s)
    classified = _classify(full_flat)
    if classified:
        return classified

    # v681 — try the LAST whitespace-separated token. The bullet's
    # canonical form is `<character_name> <mode>` (e.g. "the healer
    # on-camera"); the character name is handled by the cast: bullet
    # so here we just want the mode.
    parts = s.rsplit(None, 1)
    if len(parts) >= 2:
        last_flat = _flat(parts[-1])
        classified = _classify(last_flat)
        if classified:
            return classified

    # Unrecognized — return the raw lowercased value. Caller decides
    # whether to error or pass through to auto-detect.
    return s


def _parse_scene_blocks_legacy(md_text: str) -> List[Dict[str, Any]]:
    """Legacy 1:1 parser — each ``### Scene N`` block represents BOTH an
    image to generate AND a single-line scene in the final video. This is
    the only format the parser supported in v428-v431.

    Returns a list of scene dicts with the full set of fields:
      scene_index, prompt, reference_image, voiceover_text,
      scene_transition, clip_mode, visual_register, rhythm_tier, action_note.

    Called from the new ``parse_scene_table`` dispatcher when it detects
    no ``### Image N`` headers — in that case every ``### Scene N`` is
    assumed to be the classic combined shape.
    """
    scenes: List[Dict[str, Any]] = []

    # Find every "### Scene N" header and its following block. We capture up to
    # the next "### Scene" header or "## Summary" / end-of-file.
    scene_blocks = _re.split(r"(?=^###\s+Scene\s+\d+\s*$)", md_text, flags=_re.MULTILINE)
    for block in scene_blocks:
        header = _re.match(r"^###\s+Scene\s+(\d+)\s*$", block, flags=_re.MULTILINE)
        if not header:
            continue
        scene_index = int(header.group(1))

        # reference_image field (existing behavior)
        ref_match = _re.search(
            r"^\s*-\s*\*\*reference_image:\*\*\s*(\S+)",
            block, flags=_re.MULTILINE,
        )
        ref_value = ref_match.group(1).strip() if ref_match else "none"
        ref_parent: Optional[int] = None
        if ref_value.lower() not in ("none", "null", ""):
            m = _re.match(r"image_(\d+)", ref_value)
            if m:
                ref_parent = int(m.group(1))

        # New bullet fields — all optional
        voiceover_text = _parse_bullet_field(block, "text")
        scene_transition = _parse_bullet_field(block, "scene_transition")
        clip_mode = _parse_bullet_field(block, "clip_mode")
        rhythm_tier = _parse_bullet_field(block, "rhythm tier")
        action_note = _parse_bullet_field(block, "action_note")
        # v537 — explicit speaker mode declaration
        speaker_mode = _normalize_speaker_mode(_parse_bullet_field(block, "speaker"))

        # visual_register — the md writes things like "HOOK — bright flat ..."
        # Keep just the first token (up to the em dash) for the column,
        # but retain the full line if there's no dash.
        visual_register_raw = _parse_bullet_field(block, "visual register")
        visual_register = None
        if visual_register_raw:
            # Split on em-dash, en-dash, or regular dash+space
            parts = _re.split(r"\s+[—–-]\s+", visual_register_raw, maxsplit=1)
            visual_register = parts[0].strip() if parts else visual_register_raw.strip()

        # scene_transition may also have trailing comments — keep first token
        if scene_transition:
            scene_transition = scene_transition.split()[0].strip() if scene_transition else None
        # Normalize "null" / "None" → stored as null string for truthiness semantics
        if scene_transition and scene_transition.lower() in ("null", "none"):
            scene_transition = "null"

        # clip_mode: first token only (e.g. "blend (foo)" → "blend")
        if clip_mode:
            clip_mode = clip_mode.split()[0].strip().lower() if clip_mode else None

        # Prompt: first ```...``` fenced block after "**Image prompt...:**"
        # (the parenthetical suffix is allowed — e.g. "Image prompt (boundary image...)")
        prompt_match = _re.search(
            r"\*\*Image prompt[^*]*:\*\*\s*\n+```[a-zA-Z]*\n(.*?)\n```",
            block, flags=_re.DOTALL,
        )
        if not prompt_match:
            raise ValueError(f"Scene {scene_index}: no fenced 'Image prompt:' block found")
        prompt = prompt_match.group(1).strip()

        scenes.append({
            "scene_index": scene_index,
            "prompt": prompt,
            "reference_image": ref_parent,
            "voiceover_text": voiceover_text,
            "scene_transition": scene_transition,
            "clip_mode": clip_mode,
            "visual_register": visual_register,
            "rhythm_tier": rhythm_tier,
            "action_note": action_note,
            "speaker_mode": speaker_mode,  # v537
        })

    if not scenes:
        raise ValueError("No scenes found in the markdown (expected '### Scene N' headers)")

    # Sort by scene_index and validate references don't point to unknown scenes
    scenes.sort(key=lambda s: s["scene_index"])
    known = {s["scene_index"] for s in scenes}
    for s in scenes:
        if s["reference_image"] is not None and s["reference_image"] not in known:
            raise ValueError(
                f"Scene {s['scene_index']} references image_{s['reference_image']} which doesn't exist"
            )
        if s["reference_image"] is not None and s["reference_image"] >= s["scene_index"]:
            raise ValueError(
                f"Scene {s['scene_index']} references image_{s['reference_image']} "
                "— forward/self references not allowed"
            )

    return scenes


def _parse_image_blocks_new(md_text: str) -> List[Dict[str, Any]]:
    """New format: parse ``### Image N`` headers. Each is an image to generate.

    Returns a list of dicts with: image_index (int), prompt (str),
    reference_image (Optional[int] — another image's index). Unlike the
    legacy parser, scenes are in a separate section so these dicts
    have no voiceover/clip_mode/action_note fields.
    """
    images: List[Dict[str, Any]] = []
    # Find every "### Image N" header and its block. Capture up to the
    # next "### Image N" or "### Scene N" header, or a "## " section,
    # or end-of-file.
    blocks = _re.split(r"(?=^###\s+Image\s+\d+\s*$)", md_text, flags=_re.MULTILINE)
    for block in blocks:
        header = _re.match(r"^###\s+Image\s+(\d+)\s*$", block, flags=_re.MULTILINE)
        if not header:
            continue
        # Cut the block at the next "### Scene" / "## " header if present
        cut_m = _re.search(r"^(?:###\s+Scene\s+\d+|##\s+[A-Z])", block, flags=_re.MULTILINE)
        if cut_m and cut_m.start() > 0:
            block = block[:cut_m.start()]
        image_index = int(header.group(1))

        ref_match = _re.search(
            r"^\s*-\s*\*\*reference_image:\*\*\s*(\S+)",
            block, flags=_re.MULTILINE,
        )
        ref_value = ref_match.group(1).strip() if ref_match else "none"
        ref_parent: Optional[int] = None
        if ref_value.lower() not in ("none", "null", ""):
            m = _re.match(r"image_(\d+)", ref_value)
            if m:
                ref_parent = int(m.group(1))

        # v581: optional product_image field declares which product upload
        # this image binds. Value is the product ingredient name verbatim
        # from the Ingredients table.
        product_match = _re.search(
            r"^\s*-\s*\*\*product_image:\*\*\s*(.+?)$",
            block, flags=_re.MULTILINE,
        )
        product_image: Optional[str] = None
        if product_match:
            product_image = product_match.group(1).strip().strip("`").strip()

        prompt_match = _re.search(
            r"\*\*Image prompt[^*]*:\*\*\s*\n+```[a-zA-Z]*\n(.*?)\n```",
            block, flags=_re.DOTALL,
        )
        if not prompt_match:
            raise ValueError(f"Image {image_index}: no fenced 'Image prompt:' block found")
        prompt = prompt_match.group(1).strip()

        # v667 — per-image transformation metadata. frame_anchor_s anchors
        # this image to a source-video timestamp; visual_delta describes
        # the diff vs the prior chained image; narrative_lens is an
        # optional lens label. All three are NULL on pre-v667 imports.
        frame_anchor_match = _re.search(
            r"^\s*-\s*\*\*frame_anchor:\*\*\s*([0-9.]+)\s*s?\s*$",
            block, flags=_re.MULTILINE,
        )
        frame_anchor_s: Optional[float] = None
        if frame_anchor_match:
            try:
                frame_anchor_s = float(frame_anchor_match.group(1))
            except ValueError:
                frame_anchor_s = None

        visual_delta_match = _re.search(
            r"^\s*-\s*\*\*visual_delta:\*\*\s*(.+?)\s*$",
            block, flags=_re.MULTILINE,
        )
        visual_delta: Optional[str] = None
        if visual_delta_match:
            visual_delta = visual_delta_match.group(1).strip()

        narrative_lens_match = _re.search(
            r"^\s*-\s*\*\*narrative_lens:\*\*\s*(.+?)\s*$",
            block, flags=_re.MULTILINE,
        )
        narrative_lens: Optional[str] = None
        if narrative_lens_match:
            narrative_lens = narrative_lens_match.group(1).strip()

        if frame_anchor_s is not None or visual_delta:
            print(
                f"[v667/parse] image_{image_index} "
                f"anchor={frame_anchor_s} delta={visual_delta!r}",
                flush=True,
            )

        # v681 — per-image cast presence (optional). Comma-separated list
        # of Ingredients-table Name values present in this composition.
        # When non-empty, the binding loop in import_scene_table binds
        # ONLY these names (skipping the v509 prompt-scan fallback).
        cast_match = _re.search(
            r"^\s*-\s*\*\*cast:\*\*\s*(.+?)\s*$",
            block, flags=_re.MULTILINE,
        )
        cast_list: Optional[List[str]] = None
        if cast_match:
            raw = cast_match.group(1).strip()
            # v681 — lowercase normalization. Ingredients-table Name values
            # are matched case-insensitively at bind time but cast_json
            # is canonical lowercase so to_dict consumers don't have to
            # normalize on read.
            cast_list = [c.strip().lower() for c in raw.split(",") if c.strip()]
            if not cast_list:
                cast_list = None
        if cast_list:
            print(f"[v681/parse] image_{image_index} cast={cast_list}", flush=True)

        images.append({
            "image_index": image_index,
            "prompt": prompt,
            "reference_image": ref_parent,
            "product_image": product_image,  # v581 — None if field absent
            "frame_anchor_s": frame_anchor_s,  # v667
            "visual_delta": visual_delta,      # v667
            "narrative_lens": narrative_lens,  # v667
            "cast": cast_list,                 # v681 — None | list[str]
        })

    if not images:
        return []

    images.sort(key=lambda i: i["image_index"])
    known = {i["image_index"] for i in images}
    for i in images:
        if i["reference_image"] is not None:
            if i["reference_image"] not in known:
                raise ValueError(
                    f"Image {i['image_index']} references image_{i['reference_image']} which doesn't exist"
                )
            if i["reference_image"] >= i["image_index"]:
                raise ValueError(
                    f"Image {i['image_index']} references image_{i['reference_image']} "
                    "— forward/self references not allowed"
                )
    return images


def _parse_scene_blocks_new(md_text: str, known_image_indexes: set) -> List[Dict[str, Any]]:
    """New format: parse ``### Scene N`` headers as storyboard scenes.

    Unlike the legacy parser, these blocks don't contain an image prompt —
    they reference an image by index via ``- **image:** image_X``. Each
    scene can have multiple ``- **line:**`` bullets; each line can have
    its own ``- **action_note:**`` immediately after it.

    Returns a list of dicts:
      scene_index, image_index, clip_mode, scene_transition,
      lines (List[str]), action_notes (List[Optional[str]]),
      rhythm_tier (Optional[str]), visual_register (Optional[str]).

    Validates that every referenced image exists in ``known_image_indexes``.
    """
    scenes: List[Dict[str, Any]] = []
    blocks = _re.split(r"(?=^###\s+Scene\s+\d+\s*$)", md_text, flags=_re.MULTILINE)
    for block in blocks:
        header = _re.match(r"^###\s+Scene\s+(\d+)\s*$", block, flags=_re.MULTILINE)
        if not header:
            continue
        # Cut block at next "### " heading or "## " section boundary
        cut_m = _re.search(
            r"^(?:###\s+(?:Scene|Image)\s+\d+|##\s+[A-Z])",
            block[header.end():], flags=_re.MULTILINE,
        )
        if cut_m:
            block = block[:header.end() + cut_m.start()]
        scene_index = int(header.group(1))

        # v681 — pre-read scene_type so we can tolerate missing `image:`
        # on text_card scenes (no Nano Banana 2 image; ffmpeg drawtext
        # renders the clip directly).
        scene_type_raw = _parse_bullet_field(block, "scene_type")
        scene_type: Optional[str] = None
        if scene_type_raw:
            st = scene_type_raw.strip().lower()
            if st in ("shot", "text_card"):
                scene_type = st
        is_text_card = scene_type == "text_card"

        # Required: image reference (skipped for text_card scenes per v681)
        image_ref_m = _re.search(
            r"^\s*-\s*\*\*image:\*\*\s*(\S+)",
            block, flags=_re.MULTILINE,
        )
        if not image_ref_m:
            if is_text_card:
                image_index = None  # type: ignore[assignment]
            else:
                raise ValueError(f"Scene {scene_index}: missing '- **image:** image_N' field")
        else:
            image_ref_raw = image_ref_m.group(1).strip()
            img_m = _re.match(r"image_(\d+)", image_ref_raw)
            if not img_m:
                raise ValueError(
                    f"Scene {scene_index}: invalid image ref '{image_ref_raw}' "
                    "(expected 'image_N')"
                )
            image_index = int(img_m.group(1))
            if image_index not in known_image_indexes:
                available = sorted(known_image_indexes) if known_image_indexes else []
                raise ValueError(
                    f"Scene {scene_index} references image_{image_index} "
                    f"but no such image is defined. Available: {available}"
                )

        clip_mode = _parse_bullet_field(block, "clip_mode")
        if clip_mode:
            clip_mode = clip_mode.split()[0].strip().lower()

        scene_transition = _parse_bullet_field(block, "transition")
        if not scene_transition:
            # accept the legacy key name too
            scene_transition = _parse_bullet_field(block, "scene_transition")
        if scene_transition:
            scene_transition = scene_transition.split()[0].strip()
            if scene_transition.lower() in ("null", "none"):
                scene_transition = "null"

        rhythm_tier = _parse_bullet_field(block, "rhythm tier")

        visual_register_raw = _parse_bullet_field(block, "visual register")
        visual_register: Optional[str] = None
        if visual_register_raw:
            parts = _re.split(r"\s+[—–-]\s+", visual_register_raw, maxsplit=1)
            visual_register = (parts[0] if parts else visual_register_raw).strip()

        # v537 — explicit speaker mode declaration (per scene, applies to
        # all `- **line:**` bullets within the scene since they share the
        # same image and therefore share the same on-camera vs voiceover
        # state).
        speaker_mode = _normalize_speaker_mode(_parse_bullet_field(block, "speaker"))

        # v668 — hybrid clip cut mode (whisper | timeline | auto). NULL
        # → defaults to legacy whisper-VAD behavior. 'timeline' uses the
        # anchor-derived target_duration_s from the chained images and
        # skips whisper-VAD entirely (used for transformation montages
        # where dialogue is decorative or absent and the cut should
        # follow source-video timestamps).
        cut_mode_raw = _parse_bullet_field(block, "cut_mode")
        cut_mode: Optional[str] = None
        if cut_mode_raw:
            cm = cut_mode_raw.split()[0].strip().lower()
            if cm in ("whisper", "timeline", "auto"):
                cut_mode = cm
        if cut_mode:
            print(f"[v668/parse] scene_{scene_index} cut_mode={cut_mode}", flush=True)

        # v681 — per-scene cast presence (overrides per-image cast).
        # Comma-separated list of Ingredients-table Name values present
        # in this scene. When non-empty, image worker binds ONLY these
        # rows (skipping the v509 prompt-scan path).
        scene_cast_raw = _parse_bullet_field(block, "cast")
        scene_cast: Optional[List[str]] = None
        if scene_cast_raw:
            # v681 — lowercase normalize (same as per-image cast).
            scene_cast = [c.strip().lower() for c in scene_cast_raw.split(",") if c.strip()]
            if not scene_cast:
                scene_cast = None

        # v681 — caption (decode-side capture; generation ignores per
        # v621). On text_card scenes, this is the rendered caption text.
        caption = _parse_bullet_field(block, "caption")
        if caption:
            caption = caption.strip() or None

        # v681 — bg_color (text_card scenes only). CSS color or hex.
        bg_color = _parse_bullet_field(block, "bg_color")
        if bg_color:
            bg_color = bg_color.strip() or None

        # v681 — duration in seconds (text_card scenes only). Trailing
        # `s` tolerated. Defaults to 1.0 at render time when missing.
        duration_raw = _parse_bullet_field(block, "duration")
        duration_s: Optional[float] = None
        if duration_raw:
            cleaned = duration_raw.strip().rstrip("s").strip()
            try:
                duration_s = float(cleaned)
            except ValueError:
                duration_s = None

        if scene_type or scene_cast or caption:
            print(
                f"[v681/parse] scene_{scene_index} "
                f"type={scene_type} cast={scene_cast} caption={caption!r}",
                flush=True,
            )

        # v681 — text_card validator: required caption + bg_color;
        # forbidden image/cast/lines (soft-clear leftover bullets rather
        # than raising — be tolerant of authoring slop).
        if is_text_card:
            if not caption:
                raise ValueError(
                    f"Scene {scene_index}: scene_type=text_card requires '- **caption:**'"
                )
            if not bg_color:
                raise ValueError(
                    f"Scene {scene_index}: scene_type=text_card requires '- **bg_color:**'"
                )
            scene_cast = None

        # Parse interleaved `- **line:**` / `- **action_note:**` / `- **pad:**`
        # bullets. Order matters: action_note and pad attach to the closest
        # preceding line within the same scene.
        #
        # v644 — `pad` is an optional suffix string the platform appends to
        # the Veo dialogue line at prompt-build time. The whisper-VAD script
        # uses ONLY the `line` text (not the pad), so the pad's spoken audio
        # is automatically trimmed from the final cut as unmatched filler
        # by the existing apply_vad pipeline. Purpose: bring short lines
        # (≤9 words) up to ~20 words total so Veo 3.1's experimental audio
        # path generates speech reliably (it tends to fail on very short
        # lines, especially on Fast [Lower Priority] tier).
        #
        # We iterate through each matching bullet in source order.
        bullet_pattern = _re.compile(
            r"^\s*-\s*\*\*(line|action_note|pad)\s*:\*\*\s*(.+?)\s*$",
            flags=_re.MULTILINE | _re.IGNORECASE,
        )
        lines_list: List[str] = []
        action_notes: List[Optional[str]] = []
        pads: List[Optional[str]] = []  # v644 parallel array
        for m in bullet_pattern.finditer(block):
            key = m.group(1).lower().replace(" ", "_")
            value = m.group(2).strip()
            if key == "line":
                lines_list.append(value)
                action_notes.append(None)
                pads.append(None)
            elif key == "action_note":
                if lines_list:
                    # Attach to most recent line
                    action_notes[-1] = value
                # else: action_note before any line — ignore, likely malformed
            elif key == "pad":
                # v644 — attach pad to most recent line
                if lines_list:
                    pads[-1] = value
                # else: pad before any line — ignore, likely malformed

        # v681 — text_card scenes AND silent scenes have no `- **line:**`
        # bullets by design. Tolerate missing lines on those. Other scenes
        # (on-camera / voiceover / auto / unset speaker_mode) still
        # require at least one dialogue line (legacy behavior).
        is_silent_scene = (speaker_mode or "").lower() == "silent"
        if not lines_list and not is_text_card and not is_silent_scene:
            raise ValueError(
                f"Scene {scene_index}: no '- **line:**' bullets found "
                "(a scene must have at least one dialogue line — "
                "OR set `- **speaker:** silent` if the scene is intentionally "
                "non-speaking music/SFX/b-roll, OR set `- **scene_type:** text_card` "
                "for a text-card transition)"
            )

        scenes.append({
            "scene_index": scene_index,
            "image_index": image_index,
            "clip_mode": clip_mode,
            "scene_transition": scene_transition,
            "rhythm_tier": rhythm_tier,
            "visual_register": visual_register,
            "lines": lines_list,
            "action_notes": action_notes,
            "pads": pads,  # v644 — parallel to lines/action_notes; entries are str or None
            "speaker_mode": speaker_mode,  # v537
            "cut_mode": cut_mode,  # v668 — None | 'whisper' | 'timeline' | 'auto'
            "cast": scene_cast,           # v681 — None | list[str]
            "scene_type": scene_type,     # v681 — None | 'shot' | 'text_card'
            "caption": caption,           # v681 — source caption (decode) OR text_card text
            "bg_color": bg_color,         # v681 — text_card bg color (CSS / hex)
            "duration_s": duration_s,     # v681 — text_card duration in seconds (None → 1.0 at render)
        })

    if not scenes:
        return []
    scenes.sort(key=lambda s: s["scene_index"])
    return scenes


def _parse_ingredients_block(md_text: str) -> List[Dict[str, Any]]:
    """Parse the optional `## Ingredients` section from a scene-table markdown.

    The ingredients block declares named visual references (characters,
    products, recurring settings) that are cited by name in image prompts.
    See template_reference.md for the convention.

    v618 — HEADER-AWARE COLUMN DETECTION. Pre-v618 the parser hard-coded
    column positions as `Name | Type | Description | Source`. Authors using
    different column orders (e.g. `# | Type | Name | Reference`) silently
    produced rows with `name="1"` (the index column) and `description="the
    main character"` (the actual name) — _resolve_uploaded_ingredients
    couldn't match the persona by name, v607 force-bind silently skipped,
    and Banana 2 generated generic faces instead of the persona upload.
    Concrete failure: 2026-05-06 menopause-saffron Image 7 — the Reference
    panel showed only the saffron bottle; Image 7's variants showed a
    generic woman, not the Black-female-practitioner persona.

    v618 detects the header row's column names (case-insensitive keyword
    match against "name", "type", "description"|"desc", "source"|"reference"
    |"ref"|"path") and parses subsequent rows using the detected positions,
    accepting ANY column order. Rows where the header has columns we don't
    recognise (e.g. an `#` index column) are still parseable — those
    columns just go unused.

    Accepted shapes (both work after v618):

        ## Ingredients
        | Name | Type | Description | Source |
        |---|---|---|---|
        | the main character | character | ... | personas/refs/X.png |

    AND:

        ## Ingredients
        | # | Type | Name | Reference |
        |---|---|---|---|
        | 1 | character | the main character | personas/refs/X.png |

    Returns a list of dicts (key set unchanged for back-compat):
        [{"name": "the main character", "type": "character",
          "description": "...", "source": "..."}, ...]

    Returns [] if no Ingredients section is present, header can't be parsed,
    or no parseable rows.
    """
    # Find the "## Ingredients" section, capture until the next "## " header
    # or end of document. Case-insensitive on the heading text.
    m = _re.search(
        r"^##\s+Ingredients\s*$(.*?)(?=^##\s|\Z)",
        md_text, flags=_re.MULTILINE | _re.DOTALL | _re.IGNORECASE,
    )
    if not m:
        return []
    body = m.group(1)

    # Split body into pipe-row lines (any number of columns ≥ 2)
    pipe_row_re = _re.compile(r"^\s*\|(.+)\|\s*$", flags=_re.MULTILINE)
    rows_raw: List[List[str]] = []
    for rm in pipe_row_re.finditer(body):
        cells = [c.strip().strip("`") for c in rm.group(1).split("|")]
        rows_raw.append(cells)

    if not rows_raw:
        return []

    # Find header row — first non-divider row. A divider row is one where
    # every cell matches `^[-:\s]+$`.
    def _is_divider(cells: List[str]) -> bool:
        return all(_re.match(r"^[-:\s]*$", c) for c in cells)

    header_idx = None
    for i, cells in enumerate(rows_raw):
        if not _is_divider(cells):
            header_idx = i
            break
    if header_idx is None:
        return []
    header_cells = rows_raw[header_idx]

    # Map header keywords → position. Each output field can be filled by
    # any header containing the keyword (case-insensitive). First match
    # wins per output field.
    KEYWORDS = {
        "name": ["name"],
        "type": ["type"],
        "description": ["description", "desc"],
        "source": ["source", "reference", "ref", "path"],
    }
    col_map: Dict[str, int] = {}
    for field, keywords in KEYWORDS.items():
        for col_idx, header in enumerate(header_cells):
            h_lc = header.lower()
            if any(kw in h_lc for kw in keywords):
                col_map[field] = col_idx
                break

    if "name" not in col_map or "type" not in col_map:
        # Without name+type columns we can't produce useful rows.
        log.warning(
            f"[image_platform] _parse_ingredients_block: header row "
            f"{header_cells!r} missing 'name' or 'type' column — skipping"
        )
        return []

    name_col = col_map["name"]
    type_col = col_map["type"]
    desc_col = col_map.get("description")
    src_col = col_map.get("source")

    rows: List[Dict[str, Any]] = []
    for i, cells in enumerate(rows_raw):
        if i <= header_idx:
            continue
        if _is_divider(cells):
            continue
        # Tolerate rows with too few cells (skip them instead of crash)
        max_col = max(name_col, type_col,
                      desc_col if desc_col is not None else -1,
                      src_col if src_col is not None else -1)
        if len(cells) <= max_col:
            continue
        name = cells[name_col]
        if not name:
            continue
        # v681 — normalize em-dash, hyphen, and "(none)" placeholders in
        # the Reference column to empty string. `extra` rows declare no
        # upload via `Reference: —` per template_reference.md §"v681";
        # without this the validator at line ~4105 tries to resolve the
        # em-dash as a path and fails.
        raw_source = cells[src_col] if src_col is not None else ""
        if raw_source.strip() in ("—", "-", "(none)", "none", "null"):
            raw_source = ""
        rows.append({
            "name": name,
            "type": cells[type_col],
            "description": cells[desc_col] if desc_col is not None else "",
            "source": raw_source,
        })

    # De-dupe by name, preserving first occurrence
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for row in rows:
        if row["name"] in seen:
            continue
        seen.add(row["name"])
        deduped.append(row)
    return deduped


def parse_scene_table(md_text: str) -> Dict[str, Any]:
    """Top-level dispatcher that auto-detects the md format.

    **New format** (has ``### Image N`` headers):
        Images section and storyboard section are separate.
        Scenes reference images by index. Scenes can have multiple lines
        and may reuse images.

    **Legacy format** (only ``### Scene N`` headers, no ``### Image N``):
        Each ``### Scene N`` is BOTH an image AND a video scene. 1:1
        mapping synthesized automatically.

    Returns a dict with three lists:
        {
          "images": [ {image_index, prompt, reference_image}, ... ],
          "scenes": [ {scene_index, image_index, clip_mode, scene_transition,
                       lines, action_notes, rhythm_tier, visual_register}, ... ],
          "ingredients": [ {name, type, description, source}, ... ],   # v509, may be []
          "format": "new" | "legacy",
        }

    Raises ValueError on malformed input. Guarantees:
      - images is non-empty
      - scenes is non-empty
      - every scenes[i].image_index corresponds to an images[j].image_index
      - in new format: scene image refs are validated against defined images
      - ingredients may be [] if no ## Ingredients block is present
    """
    # v509: parse the optional ## Ingredients block first (works in both formats).
    ingredients = _parse_ingredients_block(md_text)

    # Detect format: presence of `### Image N` headers = new format
    has_image_headers = bool(_re.search(
        r"^###\s+Image\s+\d+\s*$", md_text, flags=_re.MULTILINE
    ))

    if has_image_headers:
        images = _parse_image_blocks_new(md_text)
        if not images:
            raise ValueError("Found '### Image N' header but couldn't parse any image blocks")
        known_indexes = {i["image_index"] for i in images}
        scenes = _parse_scene_blocks_new(md_text, known_indexes)
        if not scenes:
            raise ValueError(
                "Found '### Image N' blocks but no '### Scene N' storyboard blocks. "
                "Both sections are required in the new format."
            )
        # v572: optional per-clip Veo prompt overrides. Parse the
        # `## Veo 3.1 Final Prompts (per clip)` section if present and
        # attach a parallel `veo_prompts` list to each scene. Absent
        # section is fine (returns {}); malformed clip entries are
        # silently skipped (per-line fall-through to build_prompt).
        try:
            from veo_prompt_overrides import (
                parse_veo_prompts_block as _parse_veo_prompts,
                attach_veo_prompts_to_scenes as _attach_veo_prompts,
            )
            _veo_prompts_map = _parse_veo_prompts(md_text)
            _attach_veo_prompts(scenes, _veo_prompts_map)
        except ImportError:
            # Module not present on disk → graceful no-op. Existing
            # markdown without the section keeps working unchanged
            # because per-line `veo_prompts` is initialized to None.
            for s in scenes:
                s.setdefault("veo_prompts", [None] * len(s.get("lines") or []))
        return {
            "images": images,
            "scenes": scenes,
            "ingredients": ingredients,
            "format": "new",
        }

    # Legacy format: every ### Scene N is both image and scene (1:1)
    legacy = _parse_scene_blocks_legacy(md_text)
    images = [
        {
            "image_index": s["scene_index"],
            "prompt": s["prompt"],
            "reference_image": s["reference_image"],
            # v667 — legacy format predates frame_anchor; always None.
            "frame_anchor_s": None,
            "visual_delta": None,
            "narrative_lens": None,
            # v681 — legacy format predates explicit cast; always None.
            "cast": None,
        }
        for s in legacy
    ]
    scenes = [
        {
            "scene_index": s["scene_index"],
            "image_index": s["scene_index"],
            "clip_mode": s.get("clip_mode"),
            "scene_transition": s.get("scene_transition"),
            "rhythm_tier": s.get("rhythm_tier"),
            "visual_register": s.get("visual_register"),
            "lines": [s["voiceover_text"]] if s.get("voiceover_text") else [""],
            "action_notes": [s.get("action_note")],
            # v572: legacy format predates per-clip overrides — always None.
            "veo_prompts": [None],
            # v668 — legacy format predates cut_mode; always None (whisper).
            "cut_mode": None,
            # v681 — legacy format predates cast/text_card/caption; all None.
            "cast": None,
            "scene_type": None,
            "caption": None,
            "bg_color": None,
            "duration_s": None,
        }
        for s in legacy
    ]
    return {
        "images": images,
        "scenes": scenes,
        "ingredients": ingredients,
        "format": "legacy",
    }


def _parse_batch_doc_metadata(md_text: str) -> Dict[str, Any]:
    """Extract doc-level metadata from the top of the markdown.

    The format expects bold-label header lines like:
        **Video:** Title goes here
        **Persona:** Pops Hollis
        **Setting:** Tier 2 — Rustic barn workshop
        **Duration:** 60s, Option A
        **Structure:** 10 scenes, 10 images — ...
        **Video mode:** storyboard         (optional; default: storyboard)
        **Auto-split:** off                (optional; default: off)

    Returns a dict with keys: video_title, persona, setting,
    duration_seconds (int), structure, video_mode, auto_split (bool).
    All optional except the boolean, which defaults to False.
    """
    result: Dict[str, Any] = {
        "video_title": None, "persona": None, "setting": None,
        "duration_seconds": None, "structure": None,
        "video_mode": None, "auto_split": False,
    }
    patterns = [
        ("video_title",  r"^\s*\*\*Video:\*\*\s*(.+?)\s*$"),
        ("persona",      r"^\s*\*\*Persona:\*\*\s*(.+?)\s*$"),
        ("setting",      r"^\s*\*\*Setting:\*\*\s*(.+?)\s*$"),
        ("structure",    r"^\s*\*\*Structure:\*\*\s*(.+?)\s*$"),
    ]
    for key, pat in patterns:
        m = _re.search(pat, md_text, flags=_re.MULTILINE | _re.IGNORECASE)
        if m:
            result[key] = m.group(1).strip()

    dur_m = _re.search(
        r"^\s*\*\*Duration:\*\*\s*(\d+)\s*s",
        md_text, flags=_re.MULTILINE | _re.IGNORECASE,
    )
    if dur_m:
        try:
            result["duration_seconds"] = int(dur_m.group(1))
        except ValueError:
            pass

    # Video mode — match the first word/token after the colon. Accept
    # "storyboard", "auto-cycle" / "auto cycle" / "autocycle", "simple".
    # Any other value is ignored (falls back to default).
    mode_m = _re.search(
        r"^\s*\*\*Video\s*mode:\*\*\s*(.+?)\s*$",
        md_text, flags=_re.MULTILINE | _re.IGNORECASE,
    )
    if mode_m:
        raw = mode_m.group(1).strip().lower()
        # Normalize variants
        if "auto" in raw and "cycle" in raw:
            result["video_mode"] = "auto-cycle"
        elif raw.startswith("story"):
            result["video_mode"] = "storyboard"
        elif raw.startswith("simple"):
            result["video_mode"] = "simple"
        else:
            # Unknown value — log later, leave at None (caller defaults)
            pass

    # Auto-split toggle — accept common truthy/falsy spellings
    split_m = _re.search(
        r"^\s*\*\*Auto[-\s]*split:\*\*\s*(.+?)\s*$",
        md_text, flags=_re.MULTILINE | _re.IGNORECASE,
    )
    if split_m:
        raw = split_m.group(1).strip().lower()
        truthy = {"on", "yes", "true", "enabled", "enable", "y", "1"}
        falsy = {"off", "no", "false", "disabled", "disable", "n", "0"}
        if raw in truthy:
            result["auto_split"] = True
        elif raw in falsy:
            result["auto_split"] = False
        # else leave at False (the default)

    return result


class ImportSceneTableRequest(BaseModel):
    markdown: str
    subject_node_id: int  # upload node to use as subject (image 1)
    aspect_ratio: str = "9:16"
    resolution: str = "2K"
    model: str = "nano_banana_2"
    n_variants: int = Field(4, ge=1, le=4)
    name_prefix: Optional[str] = None  # e.g. "Vinegar " → "Vinegar Scene 0"
    # Text prepended to every scene's prompt, e.g. global POSITIVE/NEGATIVE
    # quality rules applicable to all scenes in this import. Separated from
    # the scene body by two newlines.
    prompt_prefix: Optional[str] = None
    # v510: when the markdown contains a `## Ingredients` block, the client
    # uploads one image per ingredient and provides this mapping from
    # ingredient name (verbatim, as declared in the ingredients table)
    # to ImageNode.id. The importer then binds each scene's parents based
    # on which ingredient names appear in that scene's prompt text, instead
    # of attaching the single subject node to every scene.
    #
    # v510 change: this mapping is now optional even when an Ingredients
    # block is declared. Any ingredient name without a mapping (and without
    # a persona alias match) gets auto-bootstrapped — the importer creates
    # a generation node using the description column as the prompt. Variants
    # (names with a parenthesized qualifier) automatically chain off their
    # base ingredient so they share face identity. See
    # _resolve_or_bootstrap_ingredients for the full logic.
    #
    # When this field is None or empty AND no Ingredients block is declared,
    # the importer falls back to the legacy single-persona path
    # (subject_node_id used as parent for every scene without a ref_image,
    # scene chain used for scenes with ref_image).
    ingredient_node_ids: Optional[Dict[str, int]] = None

    # v583: shortcut field for the product upload binding. The frontend
    # passes the product upload's node_id and the backend auto-resolves
    # which ingredient it belongs to by scanning the parsed Ingredients
    # table for the row whose `type` column is `product`. This removes
    # the need for the user to retype the product ingredient name in the
    # import form — the markdown's Ingredients table is the single source
    # of truth for the name, and the upload is the single source of truth
    # for the visual.
    #
    # When set, this is internally merged into `ingredient_node_ids` as
    # `{<product-row-name>: product_node_id}` before resolution. If the
    # markdown has no row with `type: product`, the field is ignored
    # (with a warning logged) and the import proceeds without product
    # binding — the user gets a non-fatal log entry rather than a
    # failed import.
    product_node_id: Optional[int] = None


# v510: helpers for auto-bootstrapping ingredient nodes from the markdown
# Ingredients block when explicit uploads aren't provided.

def _synthesize_bootstrap_prompt(ing: Dict[str, Any]) -> str:
    """For an ingredient declared in the table but with no upload provided,
    synthesize a clean reference-shot generation prompt from the description.

    Different ingredient types get different scaffolds. The prompt is meant
    to produce a clean isolated reference image suitable for use as a
    later ingredient in scene generations — neutral background, single
    subject, sharp focus, no other people or objects.
    """
    desc = (ing.get("description") or "").strip()
    typ = (ing.get("type") or "").lower()
    name = ing.get("name", "this subject")

    # Fall back to the name if no description was given
    body = desc or name

    if "character" in typ:
        # Works for both 'character' and 'character variant'
        return (
            f"Shot on iPhone with wide-angle lens, plain neutral light gray "
            f"studio background, soft even daylight from above. Clean "
            f"three-quarter torso portrait of {body}. Standing facing camera, "
            f"relaxed natural pose, hands at sides. Neutral expression with a "
            f"soft warm smile, looking directly at camera. Cropped at "
            f"mid-thigh — NO feet visible, NO floor visible. iPhone HDR "
            f"colors, deep focus, no other people or objects in frame."
        )
    elif "product" in typ:
        return (
            f"Studio product photography, clean white background, soft even "
            f"lighting, no harsh shadows. {body}. Centered in frame, label "
            f"fully visible and squared to camera, sharp focus. No hands, "
            f"no other objects in frame. Professional e-commerce product "
            f"shot, photorealistic."
        )
    elif "setting" in typ or "location" in typ:
        return (
            f"Shot on iPhone with wide-angle lens, handheld, deep focus "
            f"throughout, vibrant natural HDR daylight. Wide establishing "
            f"shot of {body}. No people in frame. iPhone HDR colors, "
            f"deep focus."
        )
    else:
        # Unknown type — use the description verbatim with a generic
        # iPhone-look scaffold so it stays consistent with the rest of
        # the pipeline aesthetic.
        return (
            f"Shot on iPhone with wide-angle lens, deep focus, vibrant "
            f"natural HDR daylight. {body}. Clean composition, no other "
            f"subjects in frame. iPhone HDR colors."
        )


def _detect_variant_base_name(name: str, all_names: set) -> Optional[str]:
    """If `name` is shaped like 'her daughter (before)' AND 'her daughter'
    is also in `all_names`, return 'her daughter'. Otherwise return None.

    Used to detect that two ingredient names refer to the same character
    in different states (the variant rule).
    """
    m = _re.match(r"^(.+?)\s*\([^)]+\)\s*$", name)
    if not m:
        return None
    base = m.group(1).strip()
    return base if base in all_names else None


# Recognized aliases that map an ingredient name to the persona slot.
# Case-insensitive. When an Ingredients table declares one of these, the
# importer uses the request's subject_node_id (the persona upload) as
# the parent edge for every scene that mentions it — never an anchor scene.
_PERSONA_ALIASES = {
    "the main character",
    "main character",
    "the persona",
    "persona",
    "the healer",
}


def _is_persona_alias(name: str) -> bool:
    return name.lower() in _PERSONA_ALIASES


def _resolve_uploaded_ingredients(
    db: Session,
    parsed_ingredients: List[Dict[str, Any]],
    ingredient_node_ids: Optional[Dict[str, int]],
    persona_node: "ImageNode",
    current_user: "User",
) -> Tuple[Dict[str, "ImageNode"], Dict[str, str]]:
    """v512: lighter resolver that ONLY handles uploads and persona aliases.

    Auto-bootstrap is removed in v512 — instead, the importer uses the
    "anchor scene" pattern: the first scene whose prompt mentions an
    ingredient becomes that ingredient's visual reference for all later
    scenes mentioning it. Bootstrap nodes are no longer created.

    This function returns ImageNodes only for ingredients that resolve to
    real uploads (the persona via alias match, or any ingredient explicitly
    mapped via `ingredient_node_ids`). Ingredients without a mapping are
    NOT in the returned dict — they get anchored to scene nodes during the
    per-scene loop instead.

    v573: also returns a name→type map (parallel keys) so the per-scene
    binding loop can populate `ImageEdge.kind` on upload-backed edges.
    `kind` then drives the per-slot manifest builder's role-line
    selection (persona vs product vs generic).

    Returns (Dict[ingredient_name → ImageNode], Dict[ingredient_name → type_string])
    for upload-backed ingredients.
    """
    if not parsed_ingredients:
        return {}, {}

    ingredient_node_ids = ingredient_node_ids or {}
    resolved: Dict[str, ImageNode] = {}
    types: Dict[str, str] = {}

    for ing in parsed_ingredients:
        name = ing["name"]
        ing_type = (ing.get("type") or "").strip().lower()

        # Path 1: explicit mapping in ingredient_node_ids (rare — only used
        # when the user really wants a specific upload like a real product
        # photo for label fidelity)
        node_id = ingredient_node_ids.get(name)
        if node_id is not None:
            n = db.query(ImageNode).filter(
                ImageNode.id == node_id,
                ImageNode.user_id == current_user.id,
            ).first()
            if not n:
                raise HTTPException(
                    400,
                    f"Ingredient node {node_id} (for '{name}') not found"
                )
            if n.status != "ready" or n.chosen_variant_id is None:
                raise HTTPException(
                    400,
                    f"Ingredient '{name}' (node {node_id}) is not ready with a chosen variant"
                )
            resolved[name] = n
            types[name] = ing_type or "ingredient"
            continue

        # Path 2: persona alias (e.g. "the main character") → uses the
        # subject upload directly. The persona is the only ingredient that
        # never gets anchored to a scene — she's a real photo that defines
        # her own identity, used as a reference in every scene mentioning her.
        if _is_persona_alias(name):
            resolved[name] = persona_node
            types[name] = "character"
            continue

        # Path 3: anchor mode — this ingredient will be anchored to the
        # first scene that mentions it. Don't add to `resolved`; the
        # per-scene loop handles it.

    return resolved, types


def _extract_ingredient_names_in_prompt(
    prompt_text: str,
    all_ingredient_names: List[str],
    max_matches: int = 3,
) -> List[str]:
    """For a given scene prompt, return up to `max_matches` ingredient
    names (verbatim, as declared in the table) that appear in the prompt.

    Same matching algorithm as _bind_ingredients_to_scene: case-insensitive,
    longest-first (so 'her daughter (before)' wins over 'her daughter'),
    word-boundary regex with substring fallback for names with punctuation.
    Returns names in match order.

    This is the reusable detection layer underneath both the v509-era
    upload-binding (handled by _bind_ingredients_to_scene) and the v512
    anchor-scene tracking (handled inline in the importer).
    """
    if not all_ingredient_names or not prompt_text:
        return []
    text_lc = prompt_text.lower()
    sorted_names = sorted(all_ingredient_names, key=lambda n: (-len(n), n))
    matches: List[str] = []
    consumed_spans: List[tuple] = []

    for name in sorted_names:
        name_lc = name.lower()
        if not name_lc:
            continue
        try:
            pattern = r"\b" + _re.escape(name_lc) + r"\b"
            m = _re.search(pattern, text_lc)
            if m is None:
                idx = text_lc.find(name_lc)
                if idx < 0:
                    continue
                start, end = idx, idx + len(name_lc)
            else:
                start, end = m.start(), m.end()
        except _re.error:
            idx = text_lc.find(name_lc)
            if idx < 0:
                continue
            start, end = idx, idx + len(name_lc)

        if any(cs <= start and end <= ce for cs, ce in consumed_spans):
            continue

        matches.append(name)
        consumed_spans.append((start, end))
        if len(matches) >= max_matches:
            break

    return matches




# v509: helper used by the importer to bind named ingredients to a scene
# based on which ingredient names appear in the scene's prompt text.
def _bind_ingredients_to_scene(
    prompt_text: str,
    ingredient_nodes: Dict[str, "ImageNode"],
    max_parents: int = 3,
) -> List[tuple]:
    """For a given scene prompt, return up to `max_parents` (node, role, slot_order)
    tuples for ingredients whose name appears in the prompt text.

    Matching is case-insensitive and uses word-boundary matching where
    possible. Ingredient names are checked longest-first so a longer
    name like "her daughter (before)" wins over its prefix "her daughter".

    Each ingredient is bound at most once per scene. Returns tuples in
    the order ingredients should occupy slots 0..max_parents-1.

    Important: this enforces the "never co-locate variants" rule from
    template_reference_v3.md. If both "her daughter" and
    "her daughter (before)" appear in `ingredient_nodes` and the prompt
    only mentions "her daughter (before)", only that variant is bound —
    the bare "her daughter" prefix match is suppressed because it's
    already covered by the longer-name match.
    """
    if not ingredient_nodes or not prompt_text:
        return []
    text_lc = prompt_text.lower()
    # Sort names longest-first so "her daughter (before)" outranks "her daughter"
    sorted_names = sorted(ingredient_nodes.keys(), key=lambda n: (-len(n), n))
    matches: List[tuple] = []   # list of (name, node)
    consumed_spans: List[tuple] = []  # list of (start, end) char ranges already covered

    for name in sorted_names:
        name_lc = name.lower()
        if not name_lc:
            continue
        # Prefer word-boundary match. Falls back to substring if the name
        # contains characters that make \b ambiguous (e.g. parentheses).
        try:
            pattern = r"\b" + _re.escape(name_lc) + r"\b"
            m = _re.search(pattern, text_lc)
            if m is None:
                # \b doesn't fire well around punctuation like "(before)" — try plain substring
                idx = text_lc.find(name_lc)
                if idx < 0:
                    continue
                start, end = idx, idx + len(name_lc)
            else:
                start, end = m.start(), m.end()
        except _re.error:
            idx = text_lc.find(name_lc)
            if idx < 0:
                continue
            start, end = idx, idx + len(name_lc)

        # Skip if this match is fully inside an already-consumed span
        # (e.g. "her daughter" matched after "her daughter (before)" already consumed it).
        if any(cs <= start and end <= ce for cs, ce in consumed_spans):
            continue

        matches.append((name, ingredient_nodes[name]))
        consumed_spans.append((start, end))
        if len(matches) >= max_parents:
            break

    # Return (node, role, slot_order) — slot order matches the order
    # ingredients were matched (longest-first). The role is the verbatim
    # ingredient name as it appears in the markdown / prompt.
    return [(node, name, slot) for slot, (name, node) in enumerate(matches)]


@router.post("/import-scene-table")
def import_scene_table(
    req: ImportSceneTableRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Parse a markdown scene table and create one node per scene.

    v509 — two binding modes:

    **Ingredients-based (preferred when `## Ingredients` block + `ingredient_node_ids` mapping):**
      - For each scene prompt, scan for ingredient names (longest-first,
        word-boundary) and attach matching ingredient nodes as parents.
        The role on each ImageEdge is the verbatim ingredient name.
      - If `reference_image: image_N` is also set, append the previous
        scene image as an additional parent (for setting/composition
        chain inheritance), respecting the 3-parent cap.

    **Legacy single-subject (fallback):**
      - Subject upload as parent (slot 0) — only for scenes with no `reference_image:`
      - Scene X as parent (slot 1) for scenes with `reference_image: image_X`
        (subject deliberately omitted — see comment block in code below)

    Scheduling:
      - Scenes whose parents are all ready+chosen → status="queued" (job file
        written immediately)
      - Scenes waiting on a parent → status="draft" (auto-queued later)

    v478: wrapped in top-level try/except so unexpected failures return
    a descriptive error (exception type + message) instead of FastAPI's
    generic 500 "Internal Server Error" which gave the user no signal
    about what went wrong.
    """
    try:
        return _import_scene_table_impl(req, db, current_user)
    except HTTPException:
        # Explicit 4xx/409 responses go through untouched — they already
        # have helpful detail. Rollback so a partial import doesn't
        # leave zombie rows (e.g. a half-committed batch row) when the
        # error fires after an early db.flush(). The session.close()
        # cleanup in get_db_session would normally handle this, but with
        # autocommit=False on a failed transaction Postgres can still
        # leave a row visible to other sessions briefly.
        try:
            db.rollback()
        except Exception:
            pass
        raise
    except Exception as e:
        import traceback as _tb
        tb_text = _tb.format_exc()
        # v511: explicit rollback BEFORE raising. Earlier v510 imports
        # that crashed mid-flow (e.g. UnboundLocalError after the batch
        # row was flushed but before scene nodes / bootstraps were
        # created) left zombie batches in the UI like "4 Salvora Costco"
        # — visible but with no ingredients and no way to import the
        # same script cleanly. Rolling back here keeps the DB in the
        # state it was in before this import attempt started.
        try:
            db.rollback()
        except Exception:
            pass
        # Log the full traceback server-side so we can see the call stack.
        print(f"[import_scene_table] Unexpected error: {type(e).__name__}: {e}\n{tb_text}", flush=True)
        log.error(f"[import_scene_table] Unexpected error: {type(e).__name__}: {e}")
        # Return a descriptive error to the client. Include exception
        # type + first line of message, but NOT the full traceback
        # (internal details shouldn't leak to the client).
        raise HTTPException(
            500,
            f"Import failed due to a server-side error: {type(e).__name__}: {str(e) or '(no message)'}"
        )


def _import_scene_table_impl(
    req: "ImportSceneTableRequest",
    db: Session,
    current_user: User,
):
    """Actual implementation — wrapped by import_scene_table for error
    handling. See that function's docstring for behavior."""
    # Parse first — fail early if markdown is malformed. The dispatcher
    # returns a dict { images: [...], scenes: [...], format: "new"|"legacy" }.
    # For legacy md the dispatcher auto-synthesizes a 1:1 scenes list.
    try:
        parsed = parse_scene_table(req.markdown)
    except ValueError as e:
        raise HTTPException(400, f"Parse error: {e}")

    images = parsed["images"]
    storyboard_scenes = parsed["scenes"]
    md_format = parsed["format"]

    # Validate subject exists, belongs to current user, and is a ready upload
    subject = db.query(ImageNode).filter(
        ImageNode.id == req.subject_node_id,
        ImageNode.user_id == current_user.id,
    ).first()
    if not subject:
        raise HTTPException(400, f"Subject node {req.subject_node_id} not found")
    if subject.status != "ready" or subject.chosen_variant_id is None:
        raise HTTPException(
            400,
            f"Subject node {req.subject_node_id} is not ready with a chosen variant"
        )

    # v510: ingredient resolution is deferred until after batch_id and
    # prefix are created (see the section just before PHASE 1). The
    # parsed_ingredients list itself is read here so the rest of the
    # function can rely on `parsed_ingredients` being available.
    parsed_ingredients = parsed.get("ingredients") or []
    ingredient_nodes: Dict[str, ImageNode] = {}
    # v573: parallel name→type map for upload-backed ingredients. Drives
    # the per-slot manifest builder's role-line selection.
    ingredient_types: Dict[str, str] = {}
    using_ingredients = False

    prefix = req.name_prefix or ""
    # Normalize the prompt prefix — strip trailing whitespace so we control
    # the separator (two newlines) regardless of what the user pasted.
    global_prompt_prefix = (req.prompt_prefix or "").strip()

    # v505: server-side length caps on user-entered prefix fields.
    # A user previously pasted the entire markdown (~8000 chars) into
    # the name_prefix input by accident. The importer dutifully built
    # nodes named `<entire markdown>Scene N` and the UI couldn't render
    # them — became stuck and undeletable. Reject early with a clear
    # message before doing anything destructive.
    if len(prefix) > 200:
        raise HTTPException(
            400,
            f"Name prefix is too long ({len(prefix)} chars, max 200). "
            "Did you paste the entire markdown into the wrong field? "
            "The name prefix should be a short label like '4-24.4 Liver Cleanse'."
        )
    if len(global_prompt_prefix) > 5000:
        raise HTTPException(
            400,
            f"Prompt prefix is too long ({len(global_prompt_prefix)} chars, max 5000)."
        )

    # Create a job-batch row so we can preserve the raw md and doc-level
    # metadata for the eventual "Promote to video" action. Every scene
    # node from this import will carry this batch_id.
    import uuid as _uuid
    import json as _json
    batch_id = str(_uuid.uuid4())
    doc_meta = _parse_batch_doc_metadata(req.markdown)

    # Derive a human-readable batch name. Priority:
    #   1. name_prefix typed by the user (stripped of trailing spaces/dashes)
    #   2. the "Video:" title line from the md
    #   3. fallback: first non-empty line of the md (typically the # heading)
    batch_name = (prefix or "").strip(" -—:")
    if not batch_name and doc_meta.get("video_title"):
        batch_name = doc_meta["video_title"][:200]
    if not batch_name:
        for line in req.markdown.splitlines():
            stripped = line.strip().lstrip("#").strip()
            if stripped:
                batch_name = stripped[:200]
                break
    if not batch_name:
        batch_name = "Untitled batch"

    # Duplicate-name guard: refuse the import if another ImageJobBatch
    # owned by this user already uses this name. Keeps the Images-tab
    # sidebar clean — without this, a second import with the same name
    # quietly merges under the same group key and the new scenes look
    # like they disappeared.
    #
    # Per-user scope (v447): your batch names are independent of other
    # users'. "Nightcap" belonging to user A doesn't block user B from
    # also having a "Nightcap".
    existing = db.query(ImageJobBatch).filter(
        ImageJobBatch.name == batch_name,
        ImageJobBatch.user_id == current_user.id,
    ).first()
    if existing:
        raise HTTPException(
            409,
            {
                "error": "duplicate_batch_name",
                "message": f"A batch named '{batch_name}' already exists. Pick a different name.",
                "existing_batch_id": existing.id,
                "existing_batch_name": existing.name,
                "attempted_name": batch_name,
            },
        )

    batch = ImageJobBatch(
        id=batch_id,
        user_id=current_user.id,
        name=batch_name,
        source_markdown=req.markdown,
        persona=doc_meta.get("persona"),
        setting=doc_meta.get("setting"),
        duration_seconds=doc_meta.get("duration_seconds"),
        structure=doc_meta.get("structure"),
        total_scenes=len(storyboard_scenes),
        name_prefix=(prefix or None),
        subject_node_id=subject.id,
        # Video-tab hints parsed from md (None → frontend uses defaults)
        video_mode=doc_meta.get("video_mode"),
        auto_split=bool(doc_meta.get("auto_split") or False),
    )
    db.add(batch)
    db.flush()

    # v512: Anchor-scene reference resolution.
    #
    # Each ingredient name in the Ingredients table resolves via one of two
    # paths to provide the scene generator with a visual reference:
    #
    #   1. **Upload-backed** (persona alias OR explicit ingredient_node_ids
    #      mapping): the upload becomes the parent edge, used as a Flow
    #      reference image in EVERY scene mentioning the ingredient.
    #
    #   2. **Anchor scene** (default for everything else): the FIRST scene
    #      whose prompt mentions the ingredient becomes that ingredient's
    #      visual anchor. Every later scene mentioning the same name chains
    #      its parent edge to the anchor scene, so Flow re-uses that image
    #      as the reference. No separate ingredient images are generated.
    #
    # See _resolve_uploaded_ingredients for the resolution logic.
    if parsed_ingredients:
        # v583: if the request supplied product_node_id (without a typed
        # name), auto-resolve which ingredient it belongs to by finding
        # the parsed-ingredient row whose `type` column is `product`.
        # The markdown is the single source of truth for the name; the
        # user no longer has to retype it in the import form.
        effective_node_ids = dict(req.ingredient_node_ids or {})
        if req.product_node_id is not None:
            product_rows = [
                ing for ing in parsed_ingredients
                if (ing.get("type") or "").strip().lower() == "product"
            ]
            if len(product_rows) == 1:
                product_name = product_rows[0]["name"]
                effective_node_ids[product_name] = req.product_node_id
                log.info(
                    f"[import] v583 product_node_id auto-bound to "
                    f"ingredient '{product_name}' (single product row in table)"
                )
            elif len(product_rows) == 0:
                log.warning(
                    f"[import] v583 product_node_id={req.product_node_id} "
                    f"supplied but markdown has no Ingredients row with "
                    f"type='product' — ignoring upload binding"
                )
            else:
                # Multiple product rows — ambiguous which one to bind.
                # The frontend should not be allowing this, but log and
                # use the first row's name. v583 supports only a single
                # product upload per import; multi-product would need a
                # different UI surface (per-row pickers in the import form).
                product_name = product_rows[0]["name"]
                effective_node_ids[product_name] = req.product_node_id
                log.warning(
                    f"[import] v583 markdown declares {len(product_rows)} "
                    f"product rows; binding upload to the first one "
                    f"('{product_name}'). Multi-product imports are not "
                    f"yet supported."
                )

        ingredient_nodes, ingredient_types = _resolve_uploaded_ingredients(
            db=db,
            parsed_ingredients=parsed_ingredients,
            ingredient_node_ids=effective_node_ids,
            persona_node=subject,
            current_user=current_user,
        )
        using_ingredients = True  # ingredients block present → use anchor mode
        log.info(
            f"[import] v512 ingredients mode active: {len(parsed_ingredients)} "
            f"ingredient(s) declared, {len(ingredient_nodes)} upload-backed "
            f"({list(ingredient_nodes.keys())}), "
            f"{len(parsed_ingredients) - len(ingredient_nodes)} will anchor to scenes"
        )

        # === v618b — Fail-fast validation: every character/product ingredient
        # with a declared `Reference` path MUST resolve to an uploaded
        # ImageNode. Before v618b the platform silently parsed missing
        # uploads as "anchor-scene ingredients" — Banana 2 then generated
        # generic faces / generic bottles instead of the persona / brand
        # the author intended. Concrete failure: 2026-05-06 menopause-
        # saffron Image 7 was generated WITHOUT the Black-female-
        # practitioner upload attached because the v618a parser bug had
        # registered the persona ingredient under name="1" (the # column),
        # so _resolve_uploaded_ingredients couldn't link it. v618b would
        # have caught the same scenario regardless of the v618a parser
        # bug — by checking that every character/product row with a
        # Reference path resolves to an upload. ===
        unresolved = []
        for ing in parsed_ingredients:
            ing_name = ing.get("name", "")
            ing_type = (ing.get("type") or "").strip().lower()
            ing_source = (ing.get("source") or "").strip()
            # v681 — only character + product rows REQUIRE an upload-backed
            # Reference path. `patient` rows are OPTIONAL-upload: when
            # Reference is empty (e.g. `—`), the patient enters anchor-scene
            # mode — the first scene that includes them becomes the anchor
            # and subsequent scenes chain via v512. When Reference IS set,
            # patient validates the same as character/product.
            # `extra` rows are prose-only (no upload, never anchor).
            # `setting` rows skip (existing behavior; settings may anchor).
            if ing_type not in ("character", "product"):
                # Only validate patient when its Reference is non-empty —
                # operators may legitimately ship a patient row with `—`
                # to signal "use a generated image from earlier in the job".
                if ing_type == "patient" and ing_source:
                    pass  # fall through to validation below
                else:
                    continue
            if not ing_source:
                # No Reference path declared — author may intend an
                # anchor-scene ingredient (rare for type=character/product
                # but legal). Skip validation.
                continue
            if ing_name not in ingredient_nodes:
                unresolved.append({
                    "name": ing_name,
                    "type": ing_type,
                    "source": ing_source,
                })
        if unresolved:
            details_lines = [
                f"{u['name']!r} (type={u['type']}, declared Reference: {u['source']})"
                for u in unresolved
            ]
            error_msg = (
                "Ingredient(s) with type=character/product declare a "
                "Reference path in the Ingredients table but no matching "
                "upload exists on the platform. Upload each Reference file "
                "via the Persona / Product picker UI before importing this "
                "video, OR pass `ingredient_node_ids` mapping the ingredient "
                "name → uploaded ImageNode id. Without this binding, "
                "Banana 2 will generate a generic face / generic product "
                "instead of the intended reference. Unresolved ingredients:\n"
                + "\n".join(f"  • {line}" for line in details_lines)
            )
            log.error(f"[image_platform] v618b: {error_msg}")
            raise HTTPException(status_code=400, detail=error_msg)

    # v512/v513: track which scene first introduced each non-upload ingredient.
    # Maps ingredient_name → ImageNode (the scene that first mentioned it).
    # The persona alias and explicitly-uploaded ingredients live in
    # ingredient_nodes (above); everything else gets anchored here as
    # PHASE 1 walks each image.
    anchor_scenes: Dict[str, ImageNode] = {}
    all_ingredient_names = [ing["name"] for ing in parsed_ingredients]

    # === v619 — Auto-infer + normalize image bindings ===
    # Goal: every image, regardless of how sloppy the markdown is, ends up
    # with correctly resolved bindings (character + product + chain) at
    # generation time. The author can omit the v581 binding lines, omit
    # the `product_image:` field, mention the product only in body prose
    # — v619 fills the gaps so Banana 2 always gets the right references
    # attached.
    #
    # Normalizes the `images` list IN PLACE before the binding loop runs,
    # so all downstream logic (mention extraction, edge attachment, slot
    # resolution at /worker/jobs/pending) sees consistent data.
    #
    # Operations performed per image:
    #   N1. Auto-extract `product_image` from existing v581 binding line
    #       if line is present but field is empty.
    #   N2. Auto-set `product_image` from body prose mention if any
    #       product-typed ingredient name is mentioned anywhere in the
    #       prompt and `product_image` is empty.
    #   N3. Auto-prepend the v581 product binding line if `product_image`
    #       is set but the line is missing from the prompt body.
    #   N4. Auto-prepend the v581 character binding line if missing
    #       (every image gets persona per v607).
    #   N5. Drop forward chain refs (reference_image: image_N where N >=
    #       image_index OR N not declared) — log warning, set to None.
    #
    # No HTTPException — v619 is the FEATURE-DELIVERY layer. Bad markdown
    # gets repaired; v618b's fail-fast still catches the one unrecoverable
    # case (missing upload for a declared character/product Reference).
    if using_ingredients and parsed_ingredients:
        product_ingredients = [
            ing for ing in parsed_ingredients
            if (ing.get("type") or "").strip().lower() == "product"
        ]
        product_names_lc = [
            (ing["name"].strip(), ing["name"].strip().lower())
            for ing in product_ingredients
        ]

        # Brand-keyword set: build from product ingredient names. For each
        # product like "the Korella saffron bottle", extract the brand-y
        # tokens (skip stop-words). This catches body-prose mentions like
        # "Korella saffron capsule" or just "saffron bottle".
        STOP_TOKENS = {
            "the", "a", "an", "of", "in", "on", "at", "with",
            "and", "or", "for", "to", "from", "by",
        }
        brand_keywords: List[Tuple[str, str]] = []  # (lowercase_keyword, ingredient_name)
        for ing in product_ingredients:
            name = ing["name"].strip()
            tokens = [t.strip(".,;:!?") for t in name.split()]
            for tok in tokens:
                tok_lc = tok.lower()
                if len(tok_lc) >= 4 and tok_lc not in STOP_TOKENS:
                    brand_keywords.append((tok_lc, name))

        v581_product_re = _re.compile(
            r"Use the uploaded product reference image for ([^.]+?)(?:\s*[—–-]\s*match[^.]*)?\.",
            _re.IGNORECASE,
        )
        v581_character_line = (
            "Use the uploaded character reference image for the main character."
        )

        all_image_indices = {img["image_index"] for img in images}

        for img in images:
            image_index = img["image_index"]
            body = img.get("prompt", "") or ""
            current_product_image = (img.get("product_image") or "").strip()

            # N1 — Auto-extract product_image from v581 binding line
            if not current_product_image:
                m = v581_product_re.search(body)
                if m:
                    extracted = m.group(1).strip()
                    # Match against canonical ingredient name (case-insensitive)
                    matched_canonical = next(
                        (ing["name"] for ing in product_ingredients
                         if ing["name"].lower() == extracted.lower()),
                        None
                    )
                    if matched_canonical:
                        img["product_image"] = matched_canonical
                        current_product_image = matched_canonical
                        log.info(
                            f"[image_platform] v619 N1: Image {image_index}: "
                            f"extracted product_image={matched_canonical!r} "
                            f"from existing v581 binding line"
                        )

            # N2 — Auto-set product_image from body brand-keyword mention
            if not current_product_image and brand_keywords:
                body_lc = body.lower()
                for kw_lc, ing_name in brand_keywords:
                    # Word-boundary match to avoid false positives
                    if _re.search(r"\b" + _re.escape(kw_lc) + r"\b", body_lc):
                        img["product_image"] = ing_name
                        current_product_image = ing_name
                        log.info(
                            f"[image_platform] v619 N2: Image {image_index}: "
                            f"auto-set product_image={ing_name!r} from body "
                            f"mention of '{kw_lc}'"
                        )
                        break

            # N3 — Auto-prepend v581 product binding line if missing
            if current_product_image:
                has_line = bool(v581_product_re.search(body))
                if not has_line:
                    new_line = (
                        f"Use the uploaded product reference image for "
                        f"{current_product_image}."
                    )
                    img["prompt"] = new_line + "\n" + body
                    body = img["prompt"]
                    log.info(
                        f"[image_platform] v619 N3: Image {image_index}: "
                        f"auto-prepended product binding line for "
                        f"{current_product_image!r}"
                    )

            # N4 — Auto-prepend v581 character binding line if missing
            char_re = _re.compile(
                r"Use the uploaded character reference image for [^.]+\.",
                _re.IGNORECASE,
            )
            if not char_re.search(body):
                img["prompt"] = v581_character_line + "\n" + body
                body = img["prompt"]
                log.info(
                    f"[image_platform] v619 N4: Image {image_index}: "
                    f"auto-prepended character binding line"
                )

            # N5 — Drop forward / invalid chain refs
            ref_idx = img.get("reference_image")
            if ref_idx is not None:
                if ref_idx >= image_index or ref_idx not in all_image_indices:
                    log.warning(
                        f"[image_platform] v619 N5: Image {image_index}: "
                        f"reference_image={ref_idx} is invalid "
                        f"(forward ref or undeclared) — dropping chain"
                    )
                    img["reference_image"] = None

    # v513: pre-pass to compute the anchor image index for each ingredient
    # WITHOUT creating any DB rows. This is done first so variant chains
    # can be resolved correctly: when image 1 anchors `her daughter (before)`,
    # its base `her daughter` may not be anchored until image 3. A forward-
    # lookup map lets us add an edge from image 1's node to image 3's node
    # at the moment we create image 1, even though image 3 doesn't exist
    # yet — we just store the dependency and wire it up in PHASE 2.
    #
    # anchor_image_index: ingredient_name → image_index (1-based) that anchors it.
    # Skips upload-backed ingredients (persona + explicit uploads) — those
    # don't need anchor scenes.
    anchor_image_index: Dict[str, int] = {}
    if using_ingredients:
        for img_data in sorted(images, key=lambda i: i["image_index"]):
            mentioned_pre = _extract_ingredient_names_in_prompt(
                img_data["prompt"], all_ingredient_names, max_matches=10
            )
            for ing_name in mentioned_pre:
                if ing_name in ingredient_nodes:
                    continue  # upload-backed — no anchor needed
                if ing_name in anchor_image_index:
                    continue  # already anchored by an earlier image
                anchor_image_index[ing_name] = img_data["image_index"]
        log.info(
            f"[import] v513 anchor map: "
            f"{ {n: f'image_{i}' for n, i in anchor_image_index.items()} }"
        )

    # v513: variant → base anchor index map (also computed up front).
    # When image N is the anchor for a variant ingredient (e.g. 'her daughter
    # (before)') AND the base ingredient ('her daughter') has its own anchor
    # at image M, then image N gets an extra parent edge to image M's node
    # for face/identity continuity. The variant's prompt should also include
    # a phrase like "the same person as her daughter" so nano banana
    # explicitly transfers face identity from the base reference.
    #
    # variant_base_index_for_anchor: image_index → base_image_index that the
    # variant anchored at this image should chain to.
    variant_base_index_for_anchor: Dict[int, int] = {}
    if using_ingredients:
        all_ing_names_set = set(all_ingredient_names)
        for ing_name, anchor_idx in anchor_image_index.items():
            base_name = _detect_variant_base_name(ing_name, all_ing_names_set)
            if not base_name:
                continue
            # Resolve base to either an upload-backed ingredient or its
            # anchor image. If neither exists, skip silently (writer
            # declared a variant whose base isn't actually used in any prompt).
            if base_name in ingredient_nodes:
                # base is an upload — variant chains to upload, not to a scene
                # We handle this case in PHASE 2 by adding the upload as an
                # extra parent on the variant's anchor scene.
                variant_base_index_for_anchor[anchor_idx] = -1  # sentinel: upload base
                log.info(
                    f"[import] v513 variant '{ing_name}' (anchor image_{anchor_idx}) "
                    f"will chain to upload-backed base '{base_name}' for face identity"
                )
            elif base_name in anchor_image_index:
                base_idx = anchor_image_index[base_name]
                variant_base_index_for_anchor[anchor_idx] = base_idx
                log.info(
                    f"[import] v513 variant '{ing_name}' (anchor image_{anchor_idx}) "
                    f"will chain to base '{base_name}' (anchor image_{base_idx}) "
                    f"for face identity"
                )
            else:
                log.warning(
                    f"[import] v513 variant '{ing_name}' has base '{base_name}' "
                    f"declared in ingredients table, but base is never mentioned "
                    f"in any prompt. Variant will render without face-identity chain."
                )

    # ===== PHASE 1: Create ImageNode rows, one per image_index =====
    #
    # Each `### Image N` (new format) or `### Scene N` (legacy — treated
    # as an image too) becomes one generated ImageNode. Per-node scene
    # metadata (voiceover_text, clip_mode, etc.) is populated later as a
    # denorm convenience from the *first* ImageSceneAssignment that uses
    # each image — but only in legacy 1:1 cases. For new-format imports
    # where images can be reused, the per-node scene columns get the
    # first scene's values so the Images tab can still display something
    # meaningful under each thumbnail, but the authoritative storyboard
    # data lives in ImageSceneAssignment rows.

    created_nodes_by_image_index: Dict[int, ImageNode] = {}
    queued_count = 0
    draft_count = 0

    for img in images:
        image_index = img["image_index"]
        ref_image = img["reference_image"]

        scene_body = img["prompt"].strip()
        if global_prompt_prefix:
            final_prompt = f"{scene_body}\n{global_prompt_prefix}"
        else:
            final_prompt = scene_body

        # Find the first storyboard scene that uses this image — its
        # metadata is copied down to the ImageNode as a denorm convenience
        # so the Images tab UI can show a voiceover snippet etc. under
        # each thumbnail. Authoritative video data still lives in
        # ImageSceneAssignment.
        first_scene_using = next(
            (s for s in storyboard_scenes if s["image_index"] == image_index),
            None,
        )
        denorm_voiceover = None
        denorm_clip_mode = None
        denorm_transition = None
        denorm_action_note = None
        denorm_visual_register = None
        denorm_rhythm_tier = None
        denorm_speaker_mode = None  # v537
        denorm_veo_prompt_override = None           # v572
        denorm_veo_negative_prompt_override = None  # v572
        if first_scene_using:
            lines = first_scene_using.get("lines") or []
            denorm_voiceover = lines[0] if lines else None
            denorm_clip_mode = first_scene_using.get("clip_mode")
            denorm_transition = first_scene_using.get("scene_transition")
            notes = first_scene_using.get("action_notes") or []
            denorm_action_note = notes[0] if notes else None
            denorm_visual_register = first_scene_using.get("visual_register")
            denorm_rhythm_tier = first_scene_using.get("rhythm_tier")
            denorm_speaker_mode = first_scene_using.get("speaker_mode")  # v537
            # v572 — denormalize the first clip's Veo overrides for the UI.
            # Authoritative multi-line storage is in the assignment row's
            # veo_prompts_json; this is just for the thumbnail card.
            _veo_prompts_for_scene = first_scene_using.get("veo_prompts") or []
            _first_veo = _veo_prompts_for_scene[0] if _veo_prompts_for_scene else None
            if _first_veo:
                denorm_veo_prompt_override = _first_veo.get("text_prompt") or None
                denorm_veo_negative_prompt_override = _first_veo.get("negative_prompt") or None

        node = ImageNode(
            user_id=current_user.id,
            name=f"{prefix}Scene {image_index}",
            kind="generated",
            prompt=final_prompt,
            aspect_ratio=req.aspect_ratio,
            resolution=req.resolution,
            model=req.model,
            n_variants=req.n_variants,
            status="draft",
            batch_id=batch_id,
            scene_index_in_batch=image_index,
            # Denorm from first-scene-using — nullable, for UI convenience
            voiceover_text=denorm_voiceover,
            scene_transition=denorm_transition,
            clip_mode=denorm_clip_mode,
            visual_register=denorm_visual_register,
            rhythm_tier=denorm_rhythm_tier,
            action_note=denorm_action_note,
            speaker_mode=denorm_speaker_mode,  # v537
            # v572 — first-clip Veo prompt overrides for the UI thumbnail
            veo_prompt_override=denorm_veo_prompt_override,
            veo_negative_prompt_override=denorm_veo_negative_prompt_override,
            # v667 — transformation-video metadata from the parsed image dict
            frame_anchor_s=img.get("frame_anchor_s"),
            visual_delta=img.get("visual_delta"),
            narrative_lens=img.get("narrative_lens"),
            # v681 — per-image cast (decoded list of Ingredients Name strings).
            # JSON-encoded for storage; None when the image_block didn't
            # declare a `- **cast:**` bullet.
            cast_json=(
                json.dumps(img["cast"]) if img.get("cast") else None
            ),
        )
        db.add(node)
        db.flush()
        created_nodes_by_image_index[image_index] = node

        # v509: ingredient-based binding (when ingredients block + node mapping present)
        # —————————————————————————————————————————————————————————————
        # Scan the prompt for ingredient names (longest-first) and attach
        # each matching ingredient as a parent edge with role = ingredient name.
        # If reference_image is also set, append the previous scene image as
        # an additional parent for chain inheritance, respecting the 3-parent cap.
        #
        # Legacy behavior (preserved when no ingredients block):
        # —————————————————————————————————————————————————————————————
        # v454: character-inheritance optimization
        # Scenes with a scene-reference parent (ref_image) inherit the
        # character through that reference. Including the subject ALSO
        # means Flow receives two references that depict the same person —
        # redundant at best, drift-inducing at worst (Flow tries to blend
        # two slightly-different depictions into one).
        #
        # Rule:
        #   - No ref_image → subject is the sole parent (slot 0)
        #   - Has ref_image → ref_image is the sole parent (slot 1),
        #                     subject omitted
        #
        # Slot 0 is kept reserved for "subject" to preserve the convention
        # used by backfill-from-nodes's subject-detection (line ~2574) and
        # by other future logic. When we use slot 1 only, slot 0 is
        # intentionally empty.
        if using_ingredients:
            # v512 anchor-scene mode.
            #
            # For each ingredient name found in this scene's prompt:
            #   - If it's an upload-backed ingredient (persona OR explicit
            #     upload mapping): attach the upload as the parent edge.
            #   - If it's already been anchored to an earlier scene:
            #     attach that anchor scene as the parent edge.
            #   - If neither: this scene becomes the anchor for that
            #     ingredient. Don't attach a parent for it — the scene
            #     itself defines the ingredient's appearance from this
            #     point on. Record the anchor for later scenes.
            #
            # Slots are filled in match order (longest-first). The 3-parent
            # cap still applies; if a scene mentions 4+ ingredients OR has
            # a ref_image plus 3 ingredients, the lowest-priority parent
            # is dropped with a warning.
            # v681 — when the image's parsed `cast:` bullet declares an
            # explicit list, use it verbatim (lowercased to match the
            # ingredient_names registry) and skip the v509 prompt-scan
            # heuristic. v509 stays as the fallback for legacy / cast-less
            # imports. `extra` rows have no upload (they're prose-only)
            # and yield no ingredient_node match — they get logged + skipped
            # quietly.
            parsed_image_cast = img.get("cast") if isinstance(img, dict) else None
            if parsed_image_cast:
                mentioned = []
                for cn in parsed_image_cast:
                    cn_norm = cn.strip()
                    if not cn_norm:
                        continue
                    if cn_norm in all_ingredient_names:
                        mentioned.append(cn_norm)
                    else:
                        # Tolerate case differences. Find a case-insensitive match.
                        match = next(
                            (n for n in all_ingredient_names if n.lower() == cn_norm.lower()),
                            None,
                        )
                        if match:
                            mentioned.append(match)
                        else:
                            log.info(
                                f"[v681/bind] image_{image_index}: cast='{cn_norm}' "
                                f"has no Ingredients-table match (likely extra row, "
                                f"prose-only); skipping bind"
                            )
                print(
                    f"[v681/bind] image_{image_index} declared cast={parsed_image_cast} "
                    f"→ {len(mentioned)} bound (v509 prompt-scan skipped)",
                    flush=True,
                )
            else:
                mentioned = _extract_ingredient_names_in_prompt(
                    final_prompt, all_ingredient_names, max_matches=10
                )

            # v581: if the image declares a product_image field, ensure
            # that ingredient is in the bind list even if the body scan
            # missed it. The field is the authoritative product-binding
            # signal under v581; the body scan is a fallback.
            product_image_name = img.get("product_image") if isinstance(img, dict) else None
            if product_image_name:
                if product_image_name not in ingredient_types:
                    log.warning(
                        f"[import] Image {image_index}: product_image "
                        f"'{product_image_name}' is not declared in the "
                        f"Ingredients table — binding skipped. Add it to "
                        f"the table with `type: product` to enable."
                    )
                else:
                    if product_image_name not in mentioned:
                        mentioned.append(product_image_name)
                        log.info(
                            f"[import] Image {image_index}: product_image "
                            f"field bound '{product_image_name}'"
                        )

            # v607: force-bind any character-typed ingredient even when the
            # body prose doesn't literally mention it. The character upload
            # MUST be referenced in Flow's slot manifest on every image so
            # persona-identity is preserved across the whole video. Without
            # this, when v602/v603 prose drops the literal "the main character"
            # phrase from a scene's body (e.g. a prop close-up that focuses on
            # a saffron bottle, or a chained recipe pivot), the persona ref
            # slot goes unbound and Flow generates an arbitrary face on the
            # next take. Mirrors v581 product_image binding but for the
            # persona — except characters bind unconditionally rather than
            # only when an explicit field is set, because characters have no
            # equivalent declarative field. The slot-priority sort below
            # places `character` ingredients at slot 0, so this never pushes
            # a chain or product binding out of slot bounds (3-cap still
            # honors priority).
            #
            # v681e.3 — RESPECT EXPLICIT CAST. When the operator declared
            # `cast: ...` on this image, treat it as authoritative — don't
            # force-bind a character that was deliberately omitted. v607
            # only runs as a safety net when cast: is ABSENT (v509 prompt-
            # scan path). This lets authors author scenes where the persona
            # is genuinely not present (e.g. Donna BEFORE bedroom alone in
            # the Esther/Donna testimonial — `cast: donna` excludes Esther).
            if parsed_image_cast is None:
                for _ing_name, _ing_type in ingredient_types.items():
                    if _ing_type == "character" and _ing_name in ingredient_nodes:
                        if _ing_name not in mentioned:
                            mentioned.append(_ing_name)
                            log.info(
                                f"[import] Image {image_index}: v607 force-bind "
                                f"character '{_ing_name}' (not mentioned in body)"
                            )
            else:
                # Explicit cast declared. If a character has an upload AND the
                # operator INCLUDED them in cast:, they're already in
                # `mentioned` from the v681 fast path above — no force-bind
                # needed. If they were OMITTED, respect that decision.
                included_chars = [
                    n for n, t in ingredient_types.items()
                    if t == "character" and n in mentioned
                ]
                excluded_chars = [
                    n for n, t in ingredient_types.items()
                    if t == "character" and n in ingredient_nodes and n not in mentioned
                ]
                if excluded_chars:
                    log.info(
                        f"[import] Image {image_index}: v681e.3 explicit cast — "
                        f"{len(included_chars)} character(s) bound: {included_chars}; "
                        f"{len(excluded_chars)} character(s) intentionally excluded: {excluded_chars}"
                    )

            attached_parents_count = 0
            slot = 0

            # v513: variant chain edges are DEFERRED. If this scene anchors
            # a variant ingredient (e.g. 'her daughter (before)') AND its
            # base ingredient (e.g. 'her daughter') is also being anchored
            # somewhere in this import — possibly LATER in the script —
            # then we need to attach a parent edge from this scene to the
            # base's anchor node. But the base node may not exist yet at
            # this point in the loop. So we record the dependency now and
            # wire up the edge in a final pass after PHASE 1 completes.
            #
            # When the base IS upload-backed, the upload exists already,
            # so we can attach the edge inline (slot 0, top priority).
            base_upload_for_variant: Optional[ImageNode] = None
            if image_index in variant_base_index_for_anchor:
                base_idx = variant_base_index_for_anchor[image_index]
                if base_idx == -1:
                    # base is upload-backed — attach inline as variant_chain
                    anchored_variant_name = next(
                        (n for n in mentioned
                         if n not in ingredient_nodes and n not in anchor_scenes),
                        None
                    )
                    if anchored_variant_name:
                        base_name = _detect_variant_base_name(
                            anchored_variant_name, set(all_ingredient_names)
                        )
                        if base_name and base_name in ingredient_nodes:
                            base_upload_for_variant = ingredient_nodes[base_name]

            if base_upload_for_variant is not None:
                # Attach upload-backed variant chain at slot 0 (top priority
                # for face identity).
                anchored_variant_name = next(
                    (n for n in mentioned
                     if n not in ingredient_nodes and n not in anchor_scenes),
                    "variant"
                )
                db.add(ImageEdge(
                    parent_node_id=base_upload_for_variant.id,
                    child_node_id=node.id,
                    role=f"variant_chain:{anchored_variant_name}",
                    slot_order=slot,
                    # v573: variant chains always bind to a character base
                    # (e.g. "her daughter" upload powering "her daughter
                    # (before)"). Stamp 'character' so the manifest emits
                    # the persona role line for this slot.
                    kind="character",
                ))
                attached_parents_count += 1
                slot += 1
                log.info(
                    f"[import] Image {image_index}: variant chain to upload "
                    f"'{anchored_variant_name}' base attached (slot 0)"
                )

            # v522: track which parent_node_ids have been bound for this
            # scene so we don't attach the same parent twice under
            # different ingredient labels. This catches the case where
            # multiple ingredients share an anchor scene (e.g. "her male
            # patient" and "the small banana" both first appear in image 1
            # → both anchor to image 1 → without this dedup, image 1's
            # chosen variant gets uploaded TWICE under different role
            # labels, wasting an ingredient slot and confusing Nano Banana 2
            # which gets the same image counted as two separate references).
            #
            # v520 already handled the case where reference_image's parent
            # was the same as an ingredient's parent. v522 extends that to
            # cover ingredient-vs-ingredient parent collisions.
            bound_parent_ids = set()
            if base_upload_for_variant is not None:
                bound_parent_ids.add(base_upload_for_variant.id)

            # v573: priority-sort `mentioned` so persona always wins the
            # lowest available slot, explicit uploads (product) take the
            # next slot, anchor-scene ingredients fill the remainder. This
            # gives the per-slot manifest a stable slot→role mapping
            # regardless of where the names happened to appear in the
            # prompt body. Stable sort preserves intra-class mention
            # order, so behavior in the common case (persona first, then
            # product, then anchored characters) is unchanged.
            def _slot_priority(name: str) -> int:
                if _is_persona_alias(name):
                    return 0
                if name in ingredient_nodes:
                    return 1  # explicit upload (product, etc.)
                if name in anchor_scenes:
                    return 2
                return 3  # first mention — this scene becomes anchor

            mentioned = sorted(mentioned, key=_slot_priority)

            for ing_name in mentioned:
                if attached_parents_count >= 3:
                    log.warning(
                        f"[import] Image {image_index}: 3-parent cap reached, "
                        f"can't also bind '{ing_name}'. Lower-priority bindings dropped."
                    )
                    break

                if ing_name in ingredient_nodes:
                    # Upload-backed: persona or explicit upload.
                    # Skip if this is the same upload we just attached as variant chain.
                    if (base_upload_for_variant is not None
                            and ingredient_nodes[ing_name].id == base_upload_for_variant.id):
                        continue
                    # v522: skip if another ingredient already bound this parent
                    if ingredient_nodes[ing_name].id in bound_parent_ids:
                        log.info(
                            f"[import] Image {image_index}: '{ing_name}' parent "
                            f"already bound by another ingredient — skipping duplicate"
                        )
                        continue
                    # v573: stamp `kind` from the parsed Ingredients table
                    # type column. Persona aliases get 'character'; product
                    # rows get 'product'. Drives the manifest role line.
                    edge_kind = ingredient_types.get(ing_name) or (
                        "character" if _is_persona_alias(ing_name) else None
                    )
                    db.add(ImageEdge(
                        parent_node_id=ingredient_nodes[ing_name].id,
                        child_node_id=node.id,
                        role=ing_name,
                        slot_order=slot,
                        kind=edge_kind,
                    ))
                    bound_parent_ids.add(ingredient_nodes[ing_name].id)
                    attached_parents_count += 1
                    slot += 1
                elif ing_name in anchor_scenes:
                    # Anchor scene already exists from an earlier scene.
                    # v522: skip if another ingredient already bound this parent
                    anchor_node_id = anchor_scenes[ing_name].id
                    if anchor_node_id in bound_parent_ids:
                        log.info(
                            f"[import] Image {image_index}: '{ing_name}' anchor scene "
                            f"image_{anchor_scenes[ing_name].image_index if hasattr(anchor_scenes[ing_name], 'image_index') else '?'} "
                            f"already bound by another ingredient — skipping duplicate"
                        )
                        continue
                    db.add(ImageEdge(
                        parent_node_id=anchor_node_id,
                        child_node_id=node.id,
                        role=ing_name,
                        slot_order=slot,
                    ))
                    bound_parent_ids.add(anchor_node_id)
                    attached_parents_count += 1
                    slot += 1
                else:
                    # First mention — this scene IS the anchor for this
                    # ingredient. No parent edge for it; downstream scenes
                    # will chain to THIS scene. We register the anchor
                    # AFTER db.flush() below so node.id is available.
                    pass

            # Track which ingredients to anchor to this scene (after flush)
            ingredients_to_anchor = [
                ing_name for ing_name in mentioned
                if ing_name not in ingredient_nodes and ing_name not in anchor_scenes
            ]

            # If the scene also has a ref_image and there's still room in
            # the 3-parent cap, append it for setting/composition chain.
            #
            # v520: skip if ref_image's parent_node is already bound to
            # this scene via an ingredient anchor. Previously, when a
            # scene's prompt mentioned an ingredient anchored to image_N
            # AND the scene's reference_image was also image_N, the
            # platform added TWO edges to the same parent — once with
            # the ingredient role, once with role "chain_from_image_N".
            # Nano Banana 2 then received the same image twice in its
            # reference list, wasting one of its 3 ref-image slots and
            # producing visibly degraded results (the model treats
            # duplicate refs with reduced weight). Common case: Sprouts
            # scene 8 (Salvora close-up after the reveal) anchored both
            # via "the Salvora Rhodiola Rosea bottle" AND via
            # reference_image: image_7 — both pointed at image 7.
            if ref_image is not None and attached_parents_count < 3:
                parent_node = created_nodes_by_image_index.get(ref_image)
                if parent_node is None:
                    raise HTTPException(
                        500,
                        f"Image {image_index} references image_{ref_image} which wasn't created yet"
                    )
                # v522: use the same bound_parent_ids set the ingredient
                # loop populated. Replaces the earlier db.new walk which
                # had the same effect but read worse.
                if parent_node.id in bound_parent_ids:
                    log.info(
                        f"[import] Image {image_index}: ref_image image_{ref_image} "
                        f"already bound via ingredient match — skipping duplicate chain edge"
                    )
                else:
                    db.add(ImageEdge(
                        parent_node_id=parent_node.id,
                        child_node_id=node.id,
                        role=f"chain_from_image_{ref_image}",
                        slot_order=slot,
                        # v573: prior-scene reference for setting/composition
                        # continuity — drives the chain manifest line.
                        kind="chain",
                    ))
                    bound_parent_ids.add(parent_node.id)
                    attached_parents_count += 1
                    slot += 1
            elif ref_image is not None and attached_parents_count >= 3:
                log.warning(
                    f"[import] Image {image_index}: 3 parents already bound, "
                    f"can't also chain to image_{ref_image} (3-parent cap). "
                    f"Setting/composition continuity may be reduced."
                )

            # Safety net: if the scene mentions no ingredients AND has
            # no ref_image AND won't receive a deferred variant chain
            # edge, attach the persona upload so the node has at least
            # one parent and Flow has something to anchor.
            #
            # v514 fix: previously the safety net fired for variant anchor
            # scenes (e.g. image 1 anchoring 'her daughter (before)') —
            # at the moment the per-scene loop ran, the variant chain
            # edge to image 3 hadn't been added yet (deferred to the
            # post-PHASE-1 pass). The persona then got attached as a
            # second parent on top of the variant chain, polluting the
            # reference set with an irrelevant image. Now the check
            # also looks at variant_base_index_for_anchor to predict
            # whether a deferred edge is incoming and skips the safety
            # net in that case.
            will_receive_deferred_variant_chain = (
                image_index in variant_base_index_for_anchor
                and variant_base_index_for_anchor[image_index] != -1
            )
            # v681e.7 — gate the subject-fallback safety net on the same
            # explicit-cast respect rule as v607 force-bind (line 4840).
            # When the operator declares `cast:`, that's authoritative —
            # zero attached parents is INTENTIONAL for scenes with only
            # anchor-scene patients + prose-only extras (e.g. Donna's
            # Image 1: cast=[donna, the husband], donna has no upload AND
            # is the anchor herself, husband is an extra with no upload).
            # Pre-v681e.7 the safety net fired unconditionally here and
            # attached the persona's upload as slot-0 subject — exactly
            # the wrong-character bind the user reported on Image 1.
            # Now: only run the safety net when cast: is ABSENT (legacy
            # v509 prompt-scan path).
            if (parsed_image_cast is None
                    and attached_parents_count == 0
                    and ref_image is None
                    and not will_receive_deferred_variant_chain):
                log.warning(
                    f"[import] Image {image_index}: no ingredients matched and no ref_image "
                    f"set — falling back to subject upload as parent."
                )
                db.add(ImageEdge(
                    parent_node_id=subject.id,
                    child_node_id=node.id,
                    role="subject",
                    slot_order=0,
                    # v573: subject IS the persona — stamp 'character' so
                    # the manifest emits the persona role line.
                    kind="character",
                ))
            elif (parsed_image_cast is not None
                    and attached_parents_count == 0
                    and ref_image is None
                    and not will_receive_deferred_variant_chain):
                # Explicit cast: declared but produced zero attached
                # parents — likely an anchor-scene-first scene (patient
                # has no upload AND this is her first appearance) + only
                # prose-only extras. Log as info, do NOT attach the
                # persona. Banana 2 generates from prose alone.
                log.info(
                    f"[import] Image {image_index}: v681e.7 explicit cast "
                    f"declared with no upload-backed parents (anchor-scene "
                    f"first appearance + prose-only extras). Subject-fallback "
                    f"safety net SKIPPED — Banana 2 will render from prose alone."
                )
                print(
                    f"[v681e.7] image_{image_index}: explicit cast, 0 parents — "
                    f"subject-fallback skipped",
                    flush=True,
                )
        else:
            # Legacy single-subject mode (no Ingredients block in markdown)
            if ref_image is None:
                # Parent 0: subject upload (only for scenes with no prior-scene ref)
                db.add(ImageEdge(
                    parent_node_id=subject.id,
                    child_node_id=node.id,
                    role="subject",
                    slot_order=0,
                    # v573: subject IS the persona.
                    kind="character",
                ))
            else:
                # Parent 1: previous image — character inherits through it
                parent_node = created_nodes_by_image_index.get(ref_image)
                if parent_node is None:
                    raise HTTPException(
                        500,
                        f"Image {image_index} references image_{ref_image} which wasn't created yet"
                    )
                db.add(ImageEdge(
                    parent_node_id=parent_node.id,
                    child_node_id=node.id,
                    role="reference",
                    slot_order=1,
                    # v573: prior-scene reference — stamp 'chain' so the
                    # manifest emits the chain role line.
                    kind="chain",
                ))

        db.flush()

        # v512: register this scene as the anchor for any ingredient
        # mentioned for the first time. Now that db.flush() has run,
        # node.id is available, so downstream scenes mentioning the
        # same ingredient name can chain their parent edge to this scene.
        if using_ingredients:
            for ing_name in ingredients_to_anchor:
                anchor_scenes[ing_name] = node
                log.info(
                    f"[import] Image {image_index} anchors ingredient '{ing_name}' "
                    f"(node {node.id}) — downstream scenes will chain here"
                )

        # v510 / v511 / v512: a scene starts only when ALL of its parents
        # are ready+chosen. The persona upload is always ready. Anchor
        # scenes that ARE this scene (i.e. first appearances) don't have
        # a parent edge for that ingredient at all, so no waiting. Only
        # the previously-anchored ingredients (chained to earlier scenes)
        # and the ref_image scene act as gating dependencies.
        attached_parents = []
        if using_ingredients:
            # Walk the ingredients we mentioned in this scene and resolve
            # each to its actual parent node (upload or anchor scene).
            # First mentions don't have a parent so we skip them.
            for ing_name in mentioned:
                if ing_name in ingredient_nodes:
                    attached_parents.append(ingredient_nodes[ing_name])
                elif ing_name in anchor_scenes and anchor_scenes[ing_name] is not node:
                    # Was an anchor BEFORE this scene → chained dependency.
                    # Skip if this scene is itself the anchor (just registered above).
                    attached_parents.append(anchor_scenes[ing_name])
                # Else: this scene IS the anchor → no parent edge attached
            if ref_image is not None:
                rp = created_nodes_by_image_index.get(ref_image)
                if rp is not None:
                    attached_parents.append(rp)
            # Safety-net case: no ingredients, no ref → subject upload
            if not attached_parents and ref_image is None:
                attached_parents.append(subject)
        else:
            if ref_image is None:
                attached_parents.append(subject)
            else:
                rp = created_nodes_by_image_index.get(ref_image)
                if rp is not None:
                    attached_parents.append(rp)

        can_start = True
        for parent in attached_parents:
            if parent is None or parent.status != "ready" or parent.chosen_variant_id is None:
                can_start = False
                break

        if can_start:
            node.status = "queued"
            try:
                write_generation_job(db, node)
                queued_count += 1
            except Exception as e:
                log.error(f"[import] Failed to queue image {image_index}: {e}")
                node.status = "draft"
                node.error_message = f"Queue failed: {e}"
                draft_count += 1
        else:
            draft_count += 1

    # ===== v513: deferred variant chain edges =====
    #
    # When image N anchors a variant (e.g. 'her daughter (before)') AND
    # the base ingredient (e.g. 'her daughter') is anchored at image M
    # which may come LATER in the script, we couldn't attach the parent
    # edge during the per-scene loop because image M's node didn't exist
    # yet. Now that PHASE 1 has created every scene node, we can wire up
    # the missing variant chains.
    #
    # Side effect: scenes whose variant anchor chains forward (to a later
    # image) end up with `can_start = False` because their parent isn't
    # ready+chosen yet. These scenes get demoted from queued back to draft
    # and their job file is removed. The user must pick a variant of the
    # base scene first; the cascade then auto-promotes the variant scene.
    if using_ingredients and variant_base_index_for_anchor:
        for variant_anchor_idx, base_idx in variant_base_index_for_anchor.items():
            if base_idx == -1:
                # Upload-backed: handled inline during the loop above
                continue
            variant_node = created_nodes_by_image_index.get(variant_anchor_idx)
            base_node = created_nodes_by_image_index.get(base_idx)
            if variant_node is None or base_node is None:
                continue

            # Find a free slot — variant chains take slot 0 if available,
            # else next free slot, else dropped.
            existing_edges = db.query(ImageEdge).filter(
                ImageEdge.child_node_id == variant_node.id
            ).all()
            existing_slots = {e.slot_order for e in existing_edges}
            existing_count = len(existing_edges)
            if existing_count >= 3:
                log.warning(
                    f"[import] Image {variant_anchor_idx}: 3-parent cap already "
                    f"reached, can't add variant chain to image_{base_idx}. "
                    f"Face identity may drift."
                )
                continue

            # If slot 0 is free, claim it (top priority for face identity).
            # Otherwise use the next free slot (1 or 2).
            target_slot = 0 if 0 not in existing_slots else (
                next(s for s in (1, 2) if s not in existing_slots)
            )

            # Find the variant ingredient name this scene is anchoring
            variant_name = next(
                (n for n, idx in anchor_image_index.items()
                 if idx == variant_anchor_idx
                 and _detect_variant_base_name(n, set(all_ingredient_names))),
                None
            )
            role_label = (
                f"variant_chain:{variant_name}"
                if variant_name else "variant_chain"
            )

            db.add(ImageEdge(
                parent_node_id=base_node.id,
                child_node_id=variant_node.id,
                role=role_label,
                slot_order=target_slot,
            ))
            log.info(
                f"[import] Image {variant_anchor_idx} (variant '{variant_name}') "
                f"chain attached to image_{base_idx} at slot {target_slot}"
            )

            # The variant scene now has a forward dependency. If it was
            # queued during PHASE 1 (because its other parents were ready),
            # demote it back to draft so it waits for the base scene to be
            # picked. The cascade in _promote_ready_children handles
            # auto-queueing once the base is ready+chosen.
            if variant_node.status == "queued":
                if base_node.status != "ready" or base_node.chosen_variant_id is None:
                    variant_node.status = "draft"
                    queued_count -= 1
                    draft_count += 1
                    # Also clean up the job file we wrote earlier
                    try:
                        _cleanup_job_files(variant_node.id)
                    except Exception:
                        pass
                    log.info(
                        f"[import] Image {variant_anchor_idx} demoted to draft "
                        f"(waiting on variant base image_{base_idx} to be picked)"
                    )

        db.flush()

    # ===== PHASE 2: Create ImageSceneAssignment rows =====
    #
    # One row per scene in the storyboard. Each row links a scene to an
    # ImageNode (the image to display) and captures the per-scene video
    # metadata (clip_mode, transition, lines, action_notes).

    assignments_created = 0
    for s in storyboard_scenes:
        img_idx = s["image_index"]
        node = created_nodes_by_image_index.get(img_idx)
        if node is None:
            # Shouldn't happen — parser validates image refs up-front,
            # but be defensive in case of parser bug
            raise HTTPException(
                500,
                f"Scene {s['scene_index']} references image_{img_idx} with no matching ImageNode"
            )

        # v572 — only write veo_prompts_json when the scene actually has
        # at least one override. All-null lists are stored as NULL on the
        # column to keep "no overrides" cheap (NULL takes less storage and
        # signals fall-through to the auto-build path more clearly).
        _veo_prompts_for_scene = s.get("veo_prompts") or []
        _has_any_override = any(vp for vp in _veo_prompts_for_scene)
        _veo_prompts_json_value = (
            _json.dumps(_veo_prompts_for_scene) if _has_any_override else None
        )

        # v644 — same NULL-when-empty pattern for pads_json. When a scene
        # has no `- **pad:**` bullets, store NULL (signals "no padding
        # needed for any line in this scene" → Veo prompt builder uses
        # bare line text). When at least one line has a pad, store the
        # full parallel array so per-line attribution is preserved.
        _pads_for_scene = s.get("pads") or []
        _has_any_pad = any(p for p in _pads_for_scene)
        _pads_json_value = (
            _json.dumps(_pads_for_scene) if _has_any_pad else None
        )

        # v681 — text_card scenes have no image_node binding (no Nano
        # Banana 2 render). For shot scenes the existing `node.id`
        # binding stands. For text_card the column must be nullable
        # (see migration entry below).
        scene_image_node_id = None if s.get("scene_type") == "text_card" else node.id
        assignment = ImageSceneAssignment(
            batch_id=batch_id,
            scene_index=s["scene_index"],
            image_node_id=scene_image_node_id,
            clip_mode=(s.get("clip_mode") or "blend").lower(),
            transition=s.get("scene_transition"),
            lines_json=_json.dumps(s.get("lines") or []),
            action_notes_json=_json.dumps(s.get("action_notes") or []),
            veo_prompts_json=_veo_prompts_json_value,  # v572
            pads_json=_pads_json_value,  # v644
            cut_mode=s.get("cut_mode"),  # v668 — None | 'whisper' | 'timeline' | 'auto'
            # v681 — multi-character cast + text-card metadata.
            cast_json=(_json.dumps(s["cast"]) if s.get("cast") else None),
            scene_type=s.get("scene_type"),
            caption=s.get("caption"),
            bg_color=s.get("bg_color"),
            duration_s=s.get("duration_s"),
        )
        db.add(assignment)
        assignments_created += 1

    db.commit()

    return {
        "batch_id": batch_id,
        "batch_name": batch_name,
        "format": md_format,
        "created": len(created_nodes_by_image_index),
        "queued": queued_count,
        "waiting_on_parent": draft_count,
        "scene_assignments_created": assignments_created,
        "scene_nodes": {
            str(idx): {"node_id": n.id, "status": n.status}
            for idx, n in created_nodes_by_image_index.items()
        },
    }


class BackfillBatchRequest(BaseModel):
    """Retroactively group a set of existing legacy (pre-v428) nodes into
    a new ImageJobBatch so the "Promote to video" button shows up for them.
    Per-scene metadata (voiceover_text, clip_mode, etc.) stays NULL — we
    can't recover it without the original source markdown."""
    node_ids: List[int]
    batch_name: Optional[str] = None  # if null, derived from first node's name


@router.post("/batches/backfill-from-nodes")
def backfill_batch_from_nodes(
    req: BackfillBatchRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Take a list of existing ImageNode ids and create an ImageJobBatch row
    that ties them together. Stamps batch_id + scene_index_in_batch on each
    node. Used to retrofit legacy imports that predate the batch schema.

    Rejects:
      - any node that already has a batch_id (refuse to overwrite)
      - any node not owned by the current user
      - empty list, or list with < 1 generated node
      - nodes that don't exist
      - request with more than 200 nodes (sanity cap)
    """
    if not req.node_ids:
        raise HTTPException(400, "node_ids must be a non-empty list")
    if len(req.node_ids) > 200:
        raise HTTPException(400, "Too many nodes — max 200 per backfill call")
    import uuid as _uuid

    # Only match nodes belonging to this user — prevents cross-user backfill
    nodes = db.query(ImageNode).filter(
        ImageNode.id.in_(req.node_ids),
        ImageNode.user_id == current_user.id,
    ).all()
    if len(nodes) != len(set(req.node_ids)):
        found = {n.id for n in nodes}
        missing = [nid for nid in req.node_ids if nid not in found]
        raise HTTPException(400, f"Nodes not found: {missing}")

    generated = [n for n in nodes if n.kind == "generated"]
    if not generated:
        raise HTTPException(400, "Need at least one generated node to backfill")

    already_batched = [n.id for n in generated if n.batch_id]
    if already_batched:
        raise HTTPException(
            400,
            f"Nodes already belong to a batch — won't overwrite: {already_batched}"
        )

    # Derive a sensible batch name. Priority:
    #   1. explicit req.batch_name
    #   2. the longest-common-prefix of all generated node names up to
    #      "Scene N" (e.g. "back pain man 3 " from "back pain man 3Scene 0")
    #   3. fallback: "Backfilled batch <uuid8>"
    batch_name = (req.batch_name or "").strip() or None
    if not batch_name:
        import re as _re_local
        prefixes = set()
        for n in generated:
            m = _re_local.match(r"^(.*?)Scene\s+\d+", n.name or "", _re_local.IGNORECASE)
            if m:
                p = m.group(1).rstrip(" -—:").strip()
                if p:
                    prefixes.add(p)
        if len(prefixes) == 1:
            batch_name = prefixes.pop()

    # Sort nodes by inferred scene index so scene_index_in_batch is
    # consistent with the displayed order
    def _scene_index(n):
        import re as _re_local
        m = _re_local.search(r"Scene\s+(\d+)", n.name or "", _re_local.IGNORECASE)
        return int(m.group(1)) if m else 10_000_000 + n.id

    generated_sorted = sorted(generated, key=_scene_index)

    # Try to find a subject parent (slot 0) that's common across scenes
    subject_ids = set()
    for n in generated_sorted:
        for pe in n.parent_edges:
            if (pe.slot_order or 0) == 0:
                subject_ids.add(pe.parent_node_id)
                break
    subject_node_id = subject_ids.pop() if len(subject_ids) == 1 else None

    batch_id = str(_uuid.uuid4())
    # Ensure batch name is unique. Unlike the import endpoint (which blocks
    # on collision to surface the mistake), backfill is a retroactive fix
    # and should never fail — auto-disambiguate with a numeric suffix.
    desired_name = batch_name or f"Backfilled batch {batch_id[:8]}"
    final_name = desired_name
    if db.query(ImageJobBatch).filter(
        ImageJobBatch.name == desired_name,
        ImageJobBatch.user_id == current_user.id,
    ).first():
        # Append " (2)", " (3)" etc. until we find an unused name
        for suffix in range(2, 100):
            candidate = f"{desired_name} ({suffix})"
            if not db.query(ImageJobBatch).filter(
                ImageJobBatch.name == candidate,
                ImageJobBatch.user_id == current_user.id,
            ).first():
                final_name = candidate
                break
        else:
            # 100+ collisions — fall back to the uuid-based name
            final_name = f"Backfilled batch {batch_id[:8]}"

    batch = ImageJobBatch(
        id=batch_id,
        user_id=current_user.id,
        name=final_name,
        source_markdown=None,  # legacy — we don't have it
        persona=None,
        setting=None,
        duration_seconds=None,
        structure=None,
        total_scenes=len(generated_sorted),
        name_prefix=None,
        subject_node_id=subject_node_id,
        video_mode=None,   # → frontend default (storyboard)
        auto_split=False,
    )
    db.add(batch)
    db.flush()

    for idx, n in enumerate(generated_sorted):
        n.batch_id = batch_id
        n.scene_index_in_batch = _scene_index(n) if _scene_index(n) < 10_000_000 else idx

    db.commit()
    return {
        "batch_id": batch_id,
        "batch_name": batch.name,
        "stamped_node_count": len(generated_sorted),
        "subject_node_id": subject_node_id,
    }


@router.get("/batches")
def list_batches(
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """List every ImageJobBatch owned by the current user.
    Lightweight — used by the Import modal frontend to detect name collisions
    before the user submits (so the warning surfaces instantly, without a
    round-trip for the 409).

    v461: also includes promoted_video_job_id + name_prefix so the sidebar's
    attention banner can tell which batches have been promoted to video
    (and which video job they're tied to) without an N+1 lookup per batch.

    Not paginated — batches per user are expected to stay under a few
    hundred. If that becomes a concern, add ``?limit=`` + ``?search=``.
    """
    batches = db.query(ImageJobBatch).filter(
        ImageJobBatch.user_id == current_user.id
    ).order_by(ImageJobBatch.created_at.desc()).all()
    return {
        "batches": [
            {
                "id": b.id,
                "name": b.name,
                "name_prefix": b.name_prefix,
                "promoted_video_job_id": b.promoted_video_job_id,
                "total_scenes": b.total_scenes,
            }
            for b in batches
        ],
        "total": len(batches),
    }


@router.get("/batches/{batch_id}")
def get_batch(
    batch_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Fetch batch metadata + the list of scene node ids it contains."""
    batch = db.query(ImageJobBatch).filter(
        ImageJobBatch.id == batch_id,
        ImageJobBatch.user_id == current_user.id,
    ).first()
    if not batch:
        raise HTTPException(404, f"Batch {batch_id} not found")
    nodes = db.query(ImageNode).filter(
        ImageNode.batch_id == batch_id,
        ImageNode.user_id == current_user.id,
    ).order_by(ImageNode.scene_index_in_batch).all()

    ready_count = sum(
        1 for n in nodes
        if n.kind == "generated" and n.status == "ready" and n.chosen_variant_id
    )
    total_generated = sum(1 for n in nodes if n.kind == "generated")
    return {
        **batch.to_dict(),
        "node_ids": [n.id for n in nodes],
        "ready_and_chosen_count": ready_count,
        "total_generated_count": total_generated,
        "is_promotion_eligible": ready_count == total_generated and total_generated > 0,
    }


@router.get("/batches/{batch_id}/overview")
def get_batch_overview(
    batch_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v645 — full batch overview for the operator UI.

    Returns the raw source markdown PLUS a structured breakdown by
    section so the frontend can render every part of the import in
    one place: ingredients, images, scenes (with lines/pads/action
    notes/Veo prompts per scene), and aggregate counts.

    Used by the "📋 Overview" button on the node-detail view —
    operator clicks it to inspect the entire markdown that produced
    the current image without leaving the platform.
    """
    import json as _json

    batch = db.query(ImageJobBatch).filter(
        ImageJobBatch.id == batch_id,
        ImageJobBatch.user_id == current_user.id,
    ).first()
    if not batch:
        raise HTTPException(404, f"Batch {batch_id} not found")

    md = batch.source_markdown or ""

    # Parse the structured sections directly from the stored markdown.
    # parse_scene_table returns ingredients + images + scenes already
    # broken down per the v590 / v644 format.
    parsed: Dict[str, Any] = {}
    try:
        parsed = parse_scene_table(md)
    except Exception as _pe:
        parsed = {"_parse_error": str(_pe)}

    # Pull per-scene assignment data (lines + action_notes + veo_prompts
    # + pads_json) so the overview can show what's actually stored in
    # the DB (post-import), parallel to what's in the raw markdown.
    assignments = db.query(ImageSceneAssignment).filter(
        ImageSceneAssignment.batch_id == batch_id,
    ).order_by(ImageSceneAssignment.scene_index).all()
    assignment_dicts = [a.to_dict() for a in assignments]

    # Aggregate counts for the overview header.
    images_section = parsed.get("images") or []
    scenes_section = parsed.get("scenes") or []
    ingredients_section = parsed.get("ingredients") or []
    total_lines = sum(len(s.get("lines") or []) for s in scenes_section)
    total_pads = sum(
        sum(1 for p in (s.get("pads") or []) if p)
        for s in scenes_section
    )
    total_action_notes = sum(
        sum(1 for n in (s.get("action_notes") or []) if n)
        for s in scenes_section
    )
    total_veo_prompts = sum(
        sum(1 for vp in (s.get("veo_prompts") or []) if vp)
        for s in scenes_section
    )

    return {
        **batch.to_dict(),
        "source_markdown": md,
        "source_markdown_chars": len(md),
        "parsed": {
            "ingredients": ingredients_section,
            "images": images_section,
            "scenes": scenes_section,
        },
        "assignments": assignment_dicts,
        "stats": {
            "total_images": len(images_section),
            "total_scenes": len(scenes_section),
            "total_lines": total_lines,
            "total_pads": total_pads,
            "total_action_notes": total_action_notes,
            "total_veo_prompts": total_veo_prompts,
            "total_ingredients": len(ingredients_section),
        },
    }


@router.post("/batches/{batch_id}/prepare-for-video")
def prepare_batch_for_video(
    batch_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Stage an image batch as a video-tab draft WITHOUT creating a Job or
    starting generation.

    This is the user-preferred promotion flow: the chosen variant files get
    copied into a temp upload folder (same shape as POST /api/upload), and
    the response gives the frontend everything it needs to populate the
    video-tab's storyboard review UI.

    The user then visually confirms / edits / hits Generate themselves. No
    Job/Clip rows exist until they do.

    Response (v432+):
      {
        upload_job_id: str,
        uploaded: [ {filename, original_filename, size, path, index, data_url}, ... ],
        scene_assignments: [        # canonical storyboard structure
          { scene_index, image_local_index, clip_mode, transition,
            lines: [str, ...], action_notes: [str|null, ...] },
          ...
        ],
        dialogue_lines: [str, ...],  # flattened for back-compat with simple editor
        scenes_metadata: [...],      # flattened per-line for back-compat
        video_mode, auto_split,
        batch_id, batch_name, persona, setting, duration_seconds,
      }
    """
    from shutil import copy2
    import uuid as _uuid
    import json as _json
    import base64 as _b64
    from config import app_config

    batch = db.query(ImageJobBatch).filter(
        ImageJobBatch.id == batch_id,
        ImageJobBatch.user_id == current_user.id,
    ).first()
    if not batch:
        raise HTTPException(404, f"Batch {batch_id} not found")

    nodes = db.query(ImageNode).filter(
        ImageNode.batch_id == batch_id,
        ImageNode.user_id == current_user.id,
        ImageNode.kind == "generated",
    ).order_by(ImageNode.scene_index_in_batch).all()
    if not nodes:
        raise HTTPException(400, f"Batch {batch_id} has no scene nodes")

    # Verify every scene is ready + chosen — same gate as /promote-to-video
    missing = []
    for n in nodes:
        if n.status != "ready" or n.chosen_variant_id is None:
            missing.append({
                "node_id": n.id,
                "scene_index": n.scene_index_in_batch,
                "name": n.name,
                "status": n.status,
                "has_chosen_variant": n.chosen_variant_id is not None,
            })
    if missing:
        raise HTTPException(409, {
            "error": "Not all scenes are ready to prepare",
            "missing_count": len(missing),
            "total_count": len(nodes),
            "missing": missing,
        })

    # ===== Load storyboard: prefer explicit ImageSceneAssignment rows =====
    assignments = db.query(ImageSceneAssignment).filter(
        ImageSceneAssignment.batch_id == batch_id
    ).order_by(ImageSceneAssignment.scene_index).all()

    if not assignments:
        # Legacy batch (pre-v432) — synthesize 1:1 assignments from the
        # per-node fields. Each node becomes its own one-line scene.
        synthesized: List[Dict[str, Any]] = []
        for idx, n in enumerate(nodes):
            line_text = n.voiceover_text or ""
            synthesized.append({
                "scene_index": idx,
                "image_node_id": n.id,
                "clip_mode": (n.clip_mode or "blend").lower(),
                "transition": n.scene_transition,
                "lines": [line_text],
                "action_notes": [n.action_note or None],
            })
        storyboard = synthesized
    else:
        storyboard = [a.to_dict() for a in assignments]

    # ===== Collect unique image nodes referenced by the storyboard =====
    # An image can appear in multiple scenes. We copy its file ONCE into
    # the upload folder and assign it a stable local index. Scenes
    # reference images by this local index.
    referenced_image_node_ids = []
    seen = set()
    for scene in storyboard:
        nid = scene["image_node_id"]
        # v681 — text_card scenes have image_node_id=None by design (no
        # Banana 2 render; ffmpeg drawtext renders the clip). Skip them
        # here so we don't spam orphan-assignment warnings downstream.
        if nid is None:
            continue
        if nid not in seen:
            referenced_image_node_ids.append(nid)
            seen.add(nid)

    # Build a map node_id → node object for quick lookup
    nodes_by_id = {n.id: n for n in nodes}

    # Create a fresh upload-job UUID and directory
    upload_job_id = str(_uuid.uuid4())
    upload_dir = app_config.uploads_dir / upload_job_id
    upload_dir.mkdir(parents=True, exist_ok=True)

    uploaded: List[Dict[str, Any]] = []
    node_id_to_local_index: Dict[int, int] = {}
    _media_types = {'.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp'}

    for local_idx, node_id in enumerate(referenced_image_node_ids):
        n = nodes_by_id.get(node_id)
        if n is None:
            # Orphaned assignment — references a node that no longer exists.
            # This shouldn't happen on v436+ (delete_node now merges
            # assignments before destroying the node) but it CAN happen
            # on batches that were modified on earlier versions. Skip with
            # a warning rather than 500-ing the whole promote.
            log.warning(
                f"[prepare-for-video] skipping orphaned assignment "
                f"referencing missing image_node_id={node_id} in batch {batch_id}"
            )
            continue
        variant = db.query(ImageVariant).filter(
            ImageVariant.id == n.chosen_variant_id
        ).first()
        if not variant:
            raise HTTPException(
                500, f"Node {n.id}: chosen variant {n.chosen_variant_id} missing"
            )
        src_path = images_root() / variant.image_path
        if not src_path.exists():
            # v477: try R2 rehydration before failing. Ephemeral disk on
            # Render wipes /app/data on every deploy; variant files are
            # mirrored to R2 but not kept on local disk indefinitely.
            log.info(f"[image_platform] Variant file missing locally, attempting R2 restore: {variant.image_path}")
            if not _storage_download_to_local(variant.image_path):
                raise HTTPException(
                    500,
                    f"Node {n.id}: variant file missing at {src_path} and not recoverable from R2"
                )
            if not src_path.exists():
                raise HTTPException(500, f"Node {n.id}: R2 reported success but file still missing at {src_path}")

        ext = src_path.suffix or ".png"
        new_filename = f"image_{local_idx:02d}{ext}"
        dst_path = upload_dir / new_filename
        try:
            copy2(src_path, dst_path)
        except Exception as e:
            raise HTTPException(500, f"Node {n.id}: failed to copy variant file: {e}")

        # Base64 data URL for instant frontend thumbnails (no Job row to
        # serve from yet).
        try:
            with open(dst_path, "rb") as fh:
                data_bytes = fh.read()
            data_url = (
                f"data:{_media_types.get(ext.lower(), 'image/png')};base64,"
                + _b64.b64encode(data_bytes).decode("ascii")
            )
        except Exception as e:
            log.warning(f"[prepare-for-video] data-url for {dst_path}: {e}")
            data_url = None

        uploaded.append({
            "filename": new_filename,
            "original_filename": n.name or f"scene_{local_idx}.png",
            "size": dst_path.stat().st_size if dst_path.exists() else 0,
            "path": str(dst_path),
            "index": local_idx,
            "data_url": data_url,
            "source_image_node_id": node_id,
        })
        node_id_to_local_index[node_id] = local_idx

    # ===== Build scene_assignments for the frontend =====
    # Each entry maps to one storyboard scene. image_local_index points
    # into the `uploaded[]` array. lines[] contains all dialogue lines
    # this scene owns; action_notes[] is parallel.
    #
    # v667/v668 — derive per-clip target_duration_s from frame_anchor_s
    # diffs between consecutive images in the storyboard. The lookup uses
    # the storyboard order (scene_index) so transformation chains where
    # the same image appears once per chain step still get correct deltas.
    storyboard_sorted = sorted(storyboard, key=lambda s: s["scene_index"])
    anchors_in_order: List[Tuple[int, Optional[float]]] = []
    for s in storyboard_sorted:
        n = nodes_by_id.get(s["image_node_id"])
        a = getattr(n, "frame_anchor_s", None) if n else None
        anchors_in_order.append((s["scene_index"], a))

    def _next_anchor_after(scene_idx: int) -> Optional[float]:
        for s_idx, a in anchors_in_order:
            if s_idx > scene_idx and a is not None:
                return a
        return None

    def _ceil_to_veo_bucket(dur: float) -> int:
        for b in (4, 6, 8):
            if dur <= b:
                return b
        return 8

    scene_assignments_payload: List[Dict[str, Any]] = []
    dialogue_lines_flat: List[str] = []          # back-compat (one entry per line across all scenes)
    scenes_metadata_flat: List[Dict[str, Any]] = []  # back-compat per-line rows

    # v576: flat list of {text_prompt, negative_prompt}|None entries parallel
    # to dialogue_lines_flat. The UI reads this to populate per-line Veo
    # prompt overrides on the /api/jobs payload. Pre-v576 the prepare
    # endpoint dropped veo_prompts entirely — the data was correctly stored
    # in ImageSceneAssignment.veo_prompts_json at import but never surfaced
    # to the videos tab, so /api/jobs always saw veo_prompt_override=None
    # and build_prompt's prebuilt-prompt short-circuit never fired. Result:
    # the LLM-decoded `## Veo 3.1 Final Prompts (per clip)` section was
    # parsed, stored, then silently bypassed on every promotion that went
    # through the prepare-for-video UI button (which is the only button the
    # current UI exposes).
    veo_prompts_flat: List[Optional[Dict[str, Optional[str]]]] = []
    # v644: same denorm pattern for pads (audio-padding suffix). Per-line
    # entries (str or None) parallel to dialogue_lines_flat. Forwarded
    # to /api/jobs → DialogueLineInput.dialogue_pad → Clip.dialogue_pad,
    # then Veo prompt builder appends it after the keeper line.
    pads_flat: List[Optional[str]] = []

    for scene in storyboard:
        node_id = scene["image_node_id"]
        local_idx = node_id_to_local_index.get(node_id)
        # v681 — text_card scenes have image_node_id=None by design (no
        # Banana 2 render). Don't emit the orphan warning; emit a
        # synthetic flat row downstream so the Clip writer creates one
        # text_card clip row that the video processor will render via
        # ffmpeg drawtext at export time.
        is_text_card = (scene.get("scene_type") or "").lower() == "text_card"
        if local_idx is None and not is_text_card:
            # This scene references an image node that got skipped above
            # (orphaned assignment pointing at a deleted node). Skip the
            # whole scene rather than 500-ing. The prepare flow completes
            # with whatever scenes are still valid.
            log.warning(
                f"[prepare-for-video] skipping scene {scene.get('scene_index')} "
                f"— image_node_id={node_id} not found"
            )
            continue

        lines = scene.get("lines") or []
        notes = scene.get("action_notes") or []
        # v576: assignment.to_dict() (image_platform.py:944-953) already
        # returns a `veo_prompts` list parallel to `lines` — None per line
        # when there's no override, otherwise {text_prompt, negative_prompt}.
        # The legacy synthesized branch above doesn't include this key, so
        # default to all-None.
        veo_prompts = scene.get("veo_prompts") or []
        # v644 — same parallel-array convention for pads (audio-padding
        # suffix per line; None when no pad on that line).
        pads = scene.get("pads") or []
        # Defensively zip — if asymmetric, pad notes with None to match lines
        while len(notes) < len(lines):
            notes.append(None)
        notes = notes[:len(lines)]
        while len(veo_prompts) < len(lines):
            veo_prompts.append(None)
        veo_prompts = veo_prompts[:len(lines)]
        while len(pads) < len(lines):
            pads.append(None)
        pads = pads[:len(lines)]

        clip_mode = (scene.get("clip_mode") or "blend").lower()
        transition = scene.get("transition")
        if transition in ("", "null", "None"):
            transition = None

        # v668 — per-scene cut mode (whisper | timeline | auto). NULL on
        # legacy synthesized scenes (pre-v668 imports).
        cut_mode = scene.get("cut_mode")
        # v667 — anchor-derived target duration. this_anchor → next_anchor
        # diff. None when either end is missing (no transformation chain
        # data available) — apply_vad falls back to whisper-VAD.
        this_node = nodes_by_id.get(node_id)
        this_anchor = getattr(this_node, "frame_anchor_s", None) if this_node else None
        target_duration_s: Optional[float] = None
        veo_render_duration_s: Optional[int] = None
        if this_anchor is not None:
            nxt = _next_anchor_after(scene["scene_index"])
            if nxt is not None and nxt > this_anchor:
                target_duration_s = round(nxt - this_anchor, 3)
                veo_render_duration_s = _ceil_to_veo_bucket(target_duration_s)

        # v681 — text-card / caption / cast denorm. Scene-scoped fields
        # — same value across all dialogue lines in the scene.
        scene_caption = scene.get("caption")
        scene_type_v681 = scene.get("scene_type")
        scene_bg_color = scene.get("bg_color")
        scene_duration_s = scene.get("duration_s")
        scene_cast = scene.get("cast")

        scene_assignments_payload.append({
            "scene_index": scene["scene_index"],
            "image_local_index": local_idx,
            "clip_mode": clip_mode,
            "transition": transition,
            "lines": lines,
            "action_notes": notes,
            # v576 — per-line Veo prompt overrides parallel to lines.
            "veo_prompts": veo_prompts,
            # v644 — per-line audio-padding suffixes parallel to lines.
            "pads": pads,
            # v667/v668 — transformation-video metadata for the lift composer.
            "cut_mode": cut_mode,
            "frame_anchor_s": this_anchor,
            "target_duration_s": target_duration_s,
            "veo_render_duration_s": veo_render_duration_s,
            "visual_delta": getattr(this_node, "visual_delta", None) if this_node else None,
            # v681 — multi-character cast + text-card metadata.
            "cast": scene_cast,
            "scene_type": scene_type_v681,
            "caption": scene_caption,
            "bg_color": scene_bg_color,
            "duration_s": scene_duration_s,
        })

        # v681 — scenes with no `- **line:**` bullets but a real video
        # clip needed: text_card scenes (rendered via ffmpeg drawtext)
        # AND silent scenes (Veo render with no dialogue, music/SFX or
        # b-roll under voiceover). Both need ONE Clip row downstream so
        # the renderer dispatches. Inject a synthetic flat row with
        # empty dialogue + the scene's metadata so the Clip writer
        # creates a row that the video processor will handle correctly.
        scene_speaker_mode = (scene.get("speaker_mode") or "").lower()
        scene_is_silent = scene_speaker_mode == "silent"
        scene_is_text_card = scene_type_v681 == "text_card"
        if (scene_is_text_card or scene_is_silent) and not lines:
            dialogue_lines_flat.append("")
            scenes_metadata_flat.append({
                "scene_index": scene["scene_index"],
                "line_index_in_scene": 0,
                "image_local_index": local_idx,
                "clip_mode": clip_mode,
                "transition": transition,
                "action_note": (notes[0] if notes else "") or "",
                "veo_prompt_override": None,
                "veo_negative_prompt_override": None,
                "dialogue_pad": None,
                # v667/v668 — silent scenes typically use cut_mode=timeline
                # so the clip is anchor-trimmed to a fixed duration.
                # Carry the scene's cut_mode + target_duration_s so the
                # downstream branch in video_processor handles it correctly.
                "cut_mode": cut_mode if scene_is_silent else None,
                "target_duration_s": target_duration_s if scene_is_silent else None,
                "veo_render_duration_s": veo_render_duration_s if scene_is_silent else None,
                # v681 — scene metadata. text_card carries caption+bg+duration;
                # silent scenes carry scene_type=shot (or None) so the
                # video processor doesn't try to drawtext-render them.
                "caption": scene_caption if scene_is_text_card else None,
                "scene_type": "text_card" if scene_is_text_card else (scene_type_v681 or None),
                "bg_color": scene_bg_color if scene_is_text_card else None,
                "duration_s": scene_duration_s if scene_is_text_card else None,
            })
            veo_prompts_flat.append(None)
            pads_flat.append(None)
            continue  # skip the per-line loop (no lines to iterate)

        # Back-compat flat arrays — one entry per line across all scenes
        for i_in_scene, (line_text, note, vp, pad) in enumerate(
            zip(lines, notes, veo_prompts, pads)
        ):
            dialogue_lines_flat.append(line_text or "")
            scenes_metadata_flat.append({
                "scene_index": scene["scene_index"],
                "line_index_in_scene": i_in_scene,
                "image_local_index": local_idx,
                "clip_mode": clip_mode,
                "transition": transition if i_in_scene == 0 else None,
                "action_note": note or "",
                # v576 — denorm the per-line override onto the flat row too,
                # so the UI can read it without cross-referencing scene_assignments.
                "veo_prompt_override": (vp or {}).get("text_prompt") if vp else None,
                "veo_negative_prompt_override": (vp or {}).get("negative_prompt") if vp else None,
                # v644 — denorm pad onto flat row.
                "dialogue_pad": pad,
                # v667/v668 — denorm cut_mode + anchor-derived durations onto
                # EVERY line in the scene (not just the first). All lines in
                # a scene share the same image and therefore the same
                # target_duration_s; the Clip rows downstream are 1:1 with
                # dialogue lines, so each Clip needs its own copy of the
                # cut_mode/target_duration_s to feed apply_vad's per-clip
                # branch.
                "cut_mode": cut_mode,
                "target_duration_s": target_duration_s,
                "veo_render_duration_s": veo_render_duration_s,
                # v681 — text-card / caption denorm onto the flat row.
                # Scene-scoped (same as clip_mode/transition convention):
                # only the first line of a scene carries the values; later
                # lines see None. The Clip writer in main.py handles None
                # as "no override" — text_card scenes have a single line
                # by construction so this is consistent.
                "caption": scene_caption if i_in_scene == 0 else None,
                "scene_type": scene_type_v681 if i_in_scene == 0 else None,
                "bg_color": scene_bg_color if i_in_scene == 0 else None,
            })
            veo_prompts_flat.append(vp)
            pads_flat.append(pad)

    # Resolve video_mode / auto_split — the md-parsed hints on the batch
    # take priority; otherwise default to storyboard + auto-split OFF.
    video_mode = batch.video_mode or "storyboard"
    if video_mode not in ("storyboard", "auto-cycle", "simple"):
        video_mode = "storyboard"
    auto_split = bool(batch.auto_split) if batch.auto_split is not None else False

    return {
        "upload_job_id": upload_job_id,
        "uploaded": uploaded,
        "total_uploaded": len(uploaded),
        "total_errors": 0,
        "errors": [],
        "scene_assignments": scene_assignments_payload,
        "dialogue_lines": dialogue_lines_flat,
        "scenes_metadata": scenes_metadata_flat,
        # v576 — flat list of per-line Veo prompt overrides parallel to
        # dialogue_lines. UI reads this and forwards to /api/jobs as
        # DialogueLineInput.veo_prompt_override / veo_negative_prompt_override.
        "veo_prompts": veo_prompts_flat,
        # v644 — flat list of per-line audio-padding suffixes parallel to
        # dialogue_lines. UI forwards to /api/jobs as
        # DialogueLineInput.dialogue_pad → Clip.dialogue_pad. Whisper-VAD
        # uses the bare line as script truth; Veo prompt builder appends
        # this pad after the line so Veo's audio path has enough text to
        # reliably synthesize speech (v644 docs in template_reference.md).
        "pads": pads_flat,
        "video_mode": video_mode,
        "auto_split": auto_split,
        "batch_id": batch_id,
        "batch_name": batch.name,
        "persona": batch.persona,
        "setting": batch.setting,
        "duration_seconds": batch.duration_seconds,
    }


@router.get("/batches/diagnose-match")
def diagnose_match(
    batch_id: str = Query(...),
    job_id: str = Query(...),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v569: introspection endpoint for debugging match logic.

    Dumps every field on every scene of a (batch, job) pair so we can
    see exactly what's populated and what's empty. Used to figure out
    why content matching fails even when the user knows the batch and
    job are related.

    Usage:
      GET /api/images/batches/diagnose-match?batch_id=...&job_id=...
    """
    import json as _json
    from models import Job as _Job, Clip as _Clip

    batch = db.query(ImageJobBatch).filter(
        ImageJobBatch.id == batch_id,
        ImageJobBatch.user_id == current_user.id,
    ).first()
    job = db.query(_Job).filter(
        _Job.id == job_id,
        _Job.user_id == current_user.id,
    ).first()
    if not batch or not job:
        raise HTTPException(404, "batch or job not found / not owned")

    nodes = (
        db.query(ImageNode)
        .filter(
            ImageNode.batch_id == batch_id,
            ImageNode.user_id == current_user.id,
            ImageNode.kind == "generated",
        )
        .order_by(ImageNode.scene_index_in_batch.asc())
        .all()
    )
    clips = (
        db.query(_Clip)
        .filter(_Clip.job_id == job_id)
        .order_by(_Clip.clip_index.asc())
        .all()
    )

    # Parse dialogue_json
    dialogue_lines = []
    if job.dialogue_json:
        try:
            dj = _json.loads(job.dialogue_json)
            if isinstance(dj, dict):
                dialogue_lines = dj.get("lines") or []
            elif isinstance(dj, list):
                dialogue_lines = dj
        except Exception as e:
            return {"error": f"dialogue_json parse failed: {e}", "raw": job.dialogue_json[:500]}

    # Parse config_json
    config_data = None
    if job.config_json:
        try:
            config_data = _json.loads(job.config_json)
        except Exception:
            config_data = {"_parse_error": True, "raw": job.config_json[:500]}

    # Build per-scene comparison
    scenes = []
    max_scenes = max(len(nodes), len(clips), len(dialogue_lines))
    for i in range(max_scenes):
        n = nodes[i] if i < len(nodes) else None
        c = clips[i] if i < len(clips) else None
        # Try to align dialogue_lines by scene_index/dialogue_id
        line = None
        if c and dialogue_lines:
            for dl in dialogue_lines:
                if not isinstance(dl, dict):
                    continue
                if dl.get("scene_index") == c.scene_index or dl.get("id") == c.dialogue_id:
                    line = dl
                    break
            if line is None and i < len(dialogue_lines):
                line = dialogue_lines[i]
        elif i < len(dialogue_lines) and isinstance(dialogue_lines[i], dict):
            line = dialogue_lines[i]

        scene = {
            "index": i,
            "node": (
                {
                    "id": n.id,
                    "name": n.name,
                    "scene_index_in_batch": n.scene_index_in_batch,
                    "voiceover_text": n.voiceover_text,
                    "action_note": n.action_note,
                    "visual_register": n.visual_register,
                    "rhythm_tier": n.rhythm_tier,
                    "scene_transition": n.scene_transition,
                    "clip_mode": n.clip_mode,
                    "speaker_mode": n.speaker_mode,
                    "prompt_first_200": (n.prompt or "")[:200],
                }
                if n else None
            ),
            "clip": (
                {
                    "id": c.id,
                    "clip_index": c.clip_index,
                    "scene_index": c.scene_index,
                    "dialogue_id": c.dialogue_id,
                    "dialogue_text": c.dialogue_text,
                    "clip_mode": c.clip_mode,
                    "start_frame": c.start_frame,
                    "prompt_text_first_200": (c.prompt_text or "")[:200],
                }
                if c else None
            ),
            "dialogue_json_line": line,  # may include action_note, visual_register, etc.
        }
        scenes.append(scene)

    return {
        "batch": {
            "id": batch.id,
            "name": batch.name,
            "created_at": batch.created_at.isoformat() if batch.created_at else None,
            "promoted_video_job_id": batch.promoted_video_job_id,
            "node_count": len(nodes),
        },
        "job": {
            "id": job.id,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "status": job.status,
            "backend": job.backend,
            "total_clips": job.total_clips,
            "clip_count": len(clips),
            "dialogue_json_line_count": len(dialogue_lines),
            "config_json": config_data,
            "dialogue_json_top_keys": list(_json.loads(job.dialogue_json).keys()) if job.dialogue_json else [],
        },
        "scenes": scenes,
    }


@router.post("/batches/reconcile-by-content")
def reconcile_batch_video_links_by_content(
    body: dict = Body(default={}),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v565: content-based reconciler — match batches to video jobs by
    comparing voiceover_text sequences (ImageNode side) to dialogue_text
    sequences (Clip side).

    See the actual implementation in _reconcile_by_content_impl below;
    this wrapper exists only to surface real error messages instead of
    FastAPI's generic 500 (the v565 first-deploy hit a TypeError that
    came back as 'Internal Server Error' with no diagnostics in the
    response body — wrapper fixes that).
    """
    try:
        return _reconcile_by_content_impl(body, db, current_user)
    except HTTPException:
        raise
    except Exception as e:
        import traceback as _tb
        tb_text = _tb.format_exc()
        print(f"[reconcile-by-content] Unexpected error: {type(e).__name__}: {e}\n{tb_text}", flush=True)
        log.error(f"[reconcile-by-content] Unexpected error: {type(e).__name__}: {e}")
        raise HTTPException(
            500,
            f"Reconcile failed: {type(e).__name__}: {str(e) or '(no message)'}"
        )


def _reconcile_by_content_impl(
    body: dict,
    db: Session,
    current_user: User,
):
    """v570: cascading content matcher.

    Strategy: try multiple match strategies from strictest to most
    lenient, taking the first tier that produces an unambiguous answer
    for each batch. Each match record reports which tier matched it so
    the user can audit.

    Tiers (applied per batch, first hit wins):

      Tier A — exact dialogue match: same scene count, every
        Clip.dialogue_text exactly equals the corresponding
        ImageNode.voiceover_text. This is what v567 used and what
        works for the majority of clean promotions.

      Tier B — high partial dialogue match: same scene count, >=70%
        of positions exactly match dialogue text AND clip_mode sequence
        also agrees. Catches batches where the user edited 1-2
        voiceover lines after promotion.

      Tier C — moderate partial dialogue + clip_mode + start_frame
        count: same scene count, >=50% dialogue positions match,
        clip_mode sequence agrees. Last-resort tier for heavily-edited
        cases. We require clip_mode agreement because that field is
        almost never edited and provides an independent signal.

    Ambiguity handling: if N batches all match the same job at the
    same tier, we resolve by score (sum of agreeing-fields across
    every scene). The highest-scoring batch wins the job; the others
    re-attempt against remaining unclaimed jobs at the same or lower
    tier.

    Body parameters:
      * dry_run (bool, default true): true = report only, false = write
      * overwrite_wrong (bool, default false): when true and dry_run
        false, also overwrite existing wrong promoted_video_job_id
    """
    from models import Job as _Job, Clip as _Clip
    import json as _json

    dry_run = bool(body.get("dry_run", True))
    overwrite_wrong = bool(body.get("overwrite_wrong", False))

    # ─── Step 1: gather signatures ────────────────────────────────
    # For each Job: (dialogue_text tuple, clip_mode tuple) per scene.
    # For each Batch: (voiceover_text tuple, clip_mode tuple) per scene.
    # We do NOT require dialogue_json on the Job side because v568
    # diagnosed that it's missing/partial for many real jobs.

    user_jobs = db.query(_Job).filter(_Job.user_id == current_user.id).all()
    job_by_id = {j.id: j for j in user_jobs}

    # job_id -> (dialogue_tuple, clip_mode_tuple)
    job_sigs: dict = {}
    for j in user_jobs:
        clips = (
            db.query(_Clip)
            .filter(_Clip.job_id == j.id)
            .order_by(_Clip.clip_index.asc())
            .all()
        )
        if not clips:
            continue
        dialogues = tuple((c.dialogue_text or "").strip() for c in clips)
        modes = tuple((c.clip_mode or "").strip().lower() for c in clips)
        job_sigs[j.id] = (dialogues, modes)

    user_batches = db.query(ImageJobBatch).filter(
        ImageJobBatch.user_id == current_user.id,
    ).all()

    # batch_id -> (voiceover_tuple, clip_mode_tuple)
    batch_sigs: dict = {}
    for b in user_batches:
        nodes = (
            db.query(ImageNode)
            .filter(
                ImageNode.batch_id == b.id,
                ImageNode.user_id == current_user.id,
                ImageNode.kind == "generated",
            )
            .order_by(ImageNode.scene_index_in_batch.asc())
            .all()
        )
        if not nodes:
            continue
        voiceovers = tuple((n.voiceover_text or "").strip() for n in nodes)
        modes = tuple((n.clip_mode or "").strip().lower() for n in nodes)
        batch_sigs[b.id] = (voiceovers, modes)

    # ─── Step 2: per-tier candidate scoring ───────────────────────
    def score_pair(b_dialogues, b_modes, j_dialogues, j_modes):
        """Compute (tier, score) for a (batch, job) pair, or None if no
        match at any tier. Length must already match before calling."""
        n = len(b_dialogues)
        if n == 0 or n != len(j_dialogues):
            return None
        # Dialogue exact-match count (only count NON-EMPTY-on-both)
        text_matches = 0
        text_compared = 0
        for a, c in zip(b_dialogues, j_dialogues):
            if not a and not c:
                continue
            text_compared += 1
            if a == c:
                text_matches += 1
        # If both batch and job have entirely empty dialogues, skip
        # (pathological — would match anything)
        if text_compared == 0:
            return None
        text_ratio = text_matches / text_compared
        # Mode match (categorical, expect to agree)
        mode_matches = sum(1 for a, c in zip(b_modes, j_modes) if a == c and a)
        mode_total = sum(1 for a in b_modes if a)
        mode_ratio = (mode_matches / mode_total) if mode_total else 1.0

        # Tier A: exact match on every comparable text field
        if text_ratio == 1.0 and text_matches >= 1:
            return ("A", text_matches * 100 + mode_matches)
        # Tier B: >=70% text match + mode agreement
        if text_ratio >= 0.7 and mode_ratio >= 0.8:
            return ("B", text_matches * 10 + mode_matches)
        # Tier C: >=50% text + mode agreement
        if text_ratio >= 0.5 and mode_ratio >= 0.8:
            return ("C", text_matches * 1 + mode_matches)
        return None

    # batch_id -> list of (tier, score, job_id)
    candidates_per_batch: dict = {}
    for bid, (b_dlg, b_modes) in batch_sigs.items():
        n = len(b_dlg)
        results = []
        for jid, (j_dlg, j_modes) in job_sigs.items():
            if len(j_dlg) != n:
                continue
            res = score_pair(b_dlg, b_modes, j_dlg, j_modes)
            if res is not None:
                tier, score = res
                results.append((tier, score, jid))
        if results:
            # Sort by tier (A < B < C alphabetically — A is best), then score desc
            results.sort(key=lambda t: (t[0], -t[1]))
            candidates_per_batch[bid] = results

    # ─── Step 3: assign batches to jobs, resolving conflicts ──────
    # Greedy assignment: pick the (batch, job) pair with the strongest
    # signal (best tier, then highest score) globally and lock both.
    # Then iterate. This handles the case where two batches both want
    # the same job — the stronger match wins, the other looks for its
    # next-best.

    matches = []
    assigned_jobs = set()  # job_ids already taken
    remaining_batches = set(candidates_per_batch.keys())

    # Build a flat list of all (tier, score, batch_id, job_id) records
    flat = []
    for bid, results in candidates_per_batch.items():
        for tier, score, jid in results:
            flat.append((tier, -score, bid, jid))  # negative score for ascending sort
    flat.sort()  # tier A first, highest score first

    # Walk in priority order, taking each batch+job assignment if both
    # are still free.
    batch_assignments = {}  # batch_id -> (tier, score, job_id)
    for tier, neg_score, bid, jid in flat:
        if bid in batch_assignments:
            continue  # this batch already got a higher-priority match
        if jid in assigned_jobs:
            continue  # this job already taken by another batch
        batch_assignments[bid] = (tier, -neg_score, jid)
        assigned_jobs.add(jid)

    # ─── Step 4: emit match records ───────────────────────────────
    from datetime import datetime as _dt

    for b in user_batches:
        existing = b.promoted_video_job_id
        if b.id not in batch_sigs:
            # No signature data (no generated nodes). Skip silently.
            continue

        if b.id not in batch_assignments:
            # Tried but no candidate matched at any tier
            matches.append({
                "batch_id": b.id,
                "batch_name": b.name,
                "video_job_id": None,
                "video_job_name": None,
                "scene_count": len(batch_sigs[b.id][0]),
                "confidence": "none",
                "tier": None,
                "existing_link": existing,
                "action": "skipped_no_match",
            })
            continue

        tier, score, target_jid = batch_assignments[b.id]
        target_job = job_by_id.get(target_jid)
        target_label = _job_display_label(target_job, _json) if target_job else f"job {target_jid[:8]}"

        confidence = {"A": "high", "B": "medium", "C": "low"}[tier]

        # Decide action based on existing link state
        if existing == target_jid:
            action = "skipped_existing_correct"
        elif existing is None:
            if dry_run:
                action = "would_set"
            else:
                b.promoted_video_job_id = target_jid
                action = "set"
        else:
            # existing != target — wrong link
            if not overwrite_wrong:
                action = "would_skip_existing_wrong" if dry_run else "skipped_existing_wrong"
            elif dry_run:
                action = "would_repair"
            else:
                b.promoted_video_job_id = target_jid
                action = "repaired"

        # Compute readable detail counts
        b_dlg, b_modes = batch_sigs[b.id]
        j_dlg, j_modes = job_sigs[target_jid]
        text_exact = sum(1 for x, y in zip(b_dlg, j_dlg) if x == y)
        mode_exact = sum(1 for x, y in zip(b_modes, j_modes) if x == y)

        matches.append({
            "batch_id": b.id,
            "batch_name": b.name,
            "video_job_id": target_jid,
            "video_job_name": target_label,
            "scene_count": len(b_dlg),
            "tier": tier,
            "confidence": confidence,
            "score": score,
            "text_exact_matches": text_exact,
            "mode_exact_matches": mode_exact,
            "existing_link": existing,
            "action": action,
        })

    if not dry_run:
        db.commit()
    else:
        db.rollback()

    # Tally per-tier match counts (counts each batch matched at that tier
    # regardless of whether it's set/repaired/would_*)
    matched_actions = {"set", "repaired", "would_set", "would_repair", "skipped_existing_correct"}
    by_tier = {"A": 0, "B": 0, "C": 0}
    for m in matches:
        t = m.get("tier")
        if t in by_tier and m.get("action") in matched_actions:
            by_tier[t] += 1

    return {
        "ok": True,
        "dry_run": dry_run,
        "total_batches": len(user_batches),
        "total_batches_with_data": len(batch_sigs),
        "total_jobs": len(user_jobs),
        "total_jobs_with_clips": len(job_sigs),
        "total_matched": sum(1 for m in matches if m.get("action") in ("set", "repaired", "would_set", "would_repair")),
        "by_tier": by_tier,
        "matches": matches,
    }


def _job_display_label(job, _json_module):
    """Build a human-readable label for a Job. Job has no .name attribute
    (verified models.py:144), so we synthesize one from id + an optional
    config_json hint."""
    label = f"job {job.id[:8]}"
    if job.config_json:
        try:
            cfg = _json_module.loads(job.config_json)
            if isinstance(cfg, dict):
                hint = cfg.get("imported_from_batch_name")
                if hint:
                    label = f"{hint} (job {job.id[:8]})"
        except Exception:
            pass
    return label


@router.post("/batches/{batch_id}/link-to-video-job")
def link_batch_to_video_job(
    batch_id: str,
    body: dict = Body(...),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Manually stamp the promotion link on a batch.

    v475: before this endpoint, the only way to set a batch's
    promoted_video_job_id was the full /promote-to-video flow (rare path)
    or v475's automatic stamping during create_job (only works for NEW
    jobs). Existing batches that were promoted through the normal
    "Prepare for video" flow have promoted_video_job_id=NULL forever —
    so no 🎥 badge appears.

    This endpoint lets the frontend manually link a batch to an existing
    video job. Body: {"video_job_id": "..."}. The user must own both.

    Can also be used to UNLINK by passing video_job_id=null.
    """
    batch = db.query(ImageJobBatch).filter(
        ImageJobBatch.id == batch_id,
        ImageJobBatch.user_id == current_user.id,
    ).first()
    if not batch:
        raise HTTPException(404, "Batch not found")

    video_job_id = body.get("video_job_id")
    if video_job_id:
        # Verify the video job exists and is owned by the user.
        from models import Job as _Job
        job = db.query(_Job).filter(
            _Job.id == video_job_id,
            _Job.user_id == current_user.id,
        ).first()
        if not job:
            raise HTTPException(404, "Video job not found or not owned by user")
    batch.promoted_video_job_id = video_job_id if video_job_id else None
    db.commit()
    return {
        "ok": True,
        "batch_id": batch_id,
        "promoted_video_job_id": batch.promoted_video_job_id,
    }


@router.post("/batches/backfill-video-links")
def backfill_batch_video_links(
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Auto-link batches to video jobs based on creation-time proximity.

    v475: one-time migration helper. For each of the current user's
    image batches without a promoted_video_job_id, find the closest
    video job created AFTER the batch (within a 24-hour window) that
    the user owns, and stamp the link.

    This is heuristic — a user could have created multiple jobs between
    batch creations, so "closest in time" might not always be right. But
    for the typical flow (prepare-for-video then immediately Generate),
    the correct link is the video job created within a minute or two of
    the batch. Conservative window: 24h, one batch per job (no double-
    linking). The user can manually correct via /link-to-video-job.

    v480: wrapped in try/except so unexpected failures return a
    descriptive error instead of FastAPI's generic 500. Full traceback
    goes to server logs.

    Returns a summary of what was stamped.
    """
    try:
        return _backfill_batch_video_links_impl(db, current_user)
    except HTTPException:
        raise
    except Exception as e:
        import traceback as _tb
        tb_text = _tb.format_exc()
        print(f"[backfill-video-links] Unexpected error: {type(e).__name__}: {e}\n{tb_text}", flush=True)
        log.error(f"[backfill-video-links] Unexpected error: {type(e).__name__}: {e}")
        raise HTTPException(
            500,
            f"Backfill failed due to a server-side error: {type(e).__name__}: {str(e) or '(no message)'}"
        )


@router.get("/batches/suggest-video-links")
def suggest_batch_video_links(
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v564: read-only endpoint that suggests possible video-job matches
    for currently-unlinked batches, using the strict proximity heuristic
    that used to be Tier 3 in v562's auto-backfill but WITHOUT WRITING.

    Purpose: v563 mistakenly deleted state-3 links (linked, no ground
    truth) for many users. v564's auto-backfill won't recreate them
    because Tier 3 is gone. This endpoint provides the suggestions so
    the user (or a recovery script in the browser console) can review
    them one-by-one and apply via /link-video.

    Response: { suggestions: [{ batch_id, batch_name, suggested_video_job_id,
                                 suggested_video_job_name, delta_seconds }] }
    Empty list if nothing to suggest.

    Idempotent — calling repeatedly returns the same suggestions
    (state never changes). Skips:
      - batches that are already linked
      - batches with ground truth in the index (they'll auto-link via
        /backfill-video-links)
    """
    from models import Job as _Job
    from datetime import timedelta as _td
    import json as _json

    all_jobs = db.query(_Job).filter(_Job.user_id == current_user.id).all()
    valid_job_ids = {j.id for j in all_jobs}

    # Build the same ground-truth index as the backfill so we don't
    # suggest jobs already claimed by another batch's config_json.
    batch_to_jobs: dict[str, list[str]] = {}
    jobs_claimed_by_groundtruth: set[str] = set()
    for j in all_jobs:
        if not j.config_json:
            continue
        try:
            cfg = _json.loads(j.config_json)
        except Exception:
            continue
        bid = cfg.get("imported_from_batch") if isinstance(cfg, dict) else None
        if bid:
            batch_to_jobs.setdefault(bid, []).append(j.id)
            jobs_claimed_by_groundtruth.add(j.id)

    unlinked_batches = db.query(ImageJobBatch).filter(
        ImageJobBatch.user_id == current_user.id,
        ImageJobBatch.promoted_video_job_id.is_(None),
    ).order_by(ImageJobBatch.created_at.asc()).all()

    suggestions = []
    for batch in unlinked_batches:
        # Skip batches that have ground truth — those auto-link via
        # the regular backfill, no manual action needed.
        if batch.id in batch_to_jobs:
            continue
        if not batch.created_at:
            continue

        # Find the best candidate using the v562 Tier 3 logic (within
        # 6h, assembly_mode=True, not claimed by ground truth from
        # another batch).
        best_job = None
        best_delta = None
        for j in all_jobs:
            if j.id in jobs_claimed_by_groundtruth:
                continue
            if not j.created_at or j.created_at < batch.created_at:
                continue
            delta = j.created_at - batch.created_at
            if delta > _td(hours=6):
                continue
            if not j.config_json:
                continue
            try:
                cfg = _json.loads(j.config_json)
            except Exception:
                continue
            if not isinstance(cfg, dict) or not cfg.get("assembly_mode"):
                continue
            imported_for = cfg.get("imported_from_batch")
            if imported_for and imported_for != batch.id:
                continue
            if best_delta is None or delta < best_delta:
                best_job = j
                best_delta = delta

        if best_job is not None:
            suggestions.append({
                "batch_id": batch.id,
                "batch_name": batch.name,
                "batch_created_at": batch.created_at.isoformat() if batch.created_at else None,
                "suggested_video_job_id": best_job.id,
                "suggested_video_job_name": best_job.name if hasattr(best_job, "name") else None,
                "suggested_job_created_at": best_job.created_at.isoformat() if best_job.created_at else None,
                "delta_seconds": int(best_delta.total_seconds()) if best_delta else None,
                "confidence": "low",
            })

    return {
        "ok": True,
        "suggestions": suggestions,
        "count": len(suggestions),
    }


def _backfill_batch_video_links_impl(
    db: Session,
    current_user: User,
):
    """v562: rewrite — match on ground truth (Job.config_json's
    imported_from_batch field) instead of guessing by time proximity.

    Pre-v562 the function used a time-proximity heuristic ("for each
    unlinked batch, find the closest video job created within 24h that
    isn't claimed yet"). That guessed wrong frequently — when a user
    created an image batch but never promoted it to video, the next
    unrelated video job created within 24h would get grabbed by that
    orphan batch, and the "View video job" button on a different batch
    would show the wrong video. The bad link then stuck because the
    function only processes batches where promoted_video_job_id IS NULL.

    v562 algorithm:
      1. Build a ground-truth index from every video Job in the user's
         account: Job.config_json (a JSON blob) often contains
         'imported_from_batch' which is the batch ID that created the
         video job via /promote-to-video. This is THE ground truth —
         it's set at promotion time, can't be wrong.
      2. For each batch (linked OR unlinked):
           - If a video Job claims this batch in its config: link them
             (overwriting any existing wrong link). REPAIR.
           - If the batch is unlinked AND no Job claims it AND there's
             exactly one job created within 24h after the batch that no
             other batch already owns AND has assembly_mode=True
             (signature of /promote-to-video): link with low-confidence
             flag in the response. The link IS still written — but
             marked so callers can audit if needed.
           - Otherwise: leave alone, report as 'unlinkable'.
      3. Cleanup: if a batch points at a video Job that no longer exists
         (deleted), clear the link (it's stale).
    """
    from models import Job as _Job
    from datetime import timedelta as _td
    import json as _json

    # Step 1: build the ground-truth index from Job.config_json
    # ─────────────────────────────────────────────────────────
    # imported_from_batch is set in promote_batch_to_video at line ~4937.
    # Format: config_json is a JSON string; parse it and look up the key.
    all_jobs = db.query(_Job).filter(
        _Job.user_id == current_user.id,
    ).all()

    # batch_id (str) → list of Job.id (str) that claim it
    # Normally len() == 1 but defensive in case of dupes from a
    # buggy retry or manual SQL fixup.
    batch_to_jobs: dict[str, list[str]] = {}
    valid_job_ids: set[str] = set()
    for j in all_jobs:
        valid_job_ids.add(j.id)
        if not j.config_json:
            continue
        try:
            cfg = _json.loads(j.config_json)
        except Exception:
            continue
        bid = cfg.get("imported_from_batch") if isinstance(cfg, dict) else None
        if bid:
            batch_to_jobs.setdefault(bid, []).append(j.id)

    # Step 2: walk every batch (linked AND unlinked) and compute the
    # correct value. We process linked batches too so we can REPAIR
    # bad links from the pre-v562 time-proximity heuristic.
    # ─────────────────────────────────────────────────────────
    all_batches = db.query(ImageJobBatch).filter(
        ImageJobBatch.user_id == current_user.id,
    ).order_by(ImageJobBatch.created_at.asc()).all()

    details = []
    repaired = 0
    linked_groundtruth = 0
    linked_proximity = 0
    cleared_stale = 0
    unlinkable = 0

    # Track which jobs are claimed by a batch via ground-truth so we
    # don't fall back to assigning them via proximity (those are taken).
    jobs_claimed_by_groundtruth: set[str] = set()
    for bid, jids in batch_to_jobs.items():
        for jid in jids:
            jobs_claimed_by_groundtruth.add(jid)

    for batch in all_batches:
        existing_link = batch.promoted_video_job_id
        groundtruth_jobs = batch_to_jobs.get(batch.id, [])

        # Case A: ground truth says this batch goes with one specific Job
        if groundtruth_jobs:
            # Pick the most recently created Job among matches (defensive
            # against dupes; in practice there's only one).
            best = sorted(
                [j for j in all_jobs if j.id in groundtruth_jobs],
                key=lambda j: j.created_at or batch.created_at,
                reverse=True,
            )[0]
            target = best.id
            if existing_link != target:
                if existing_link is None:
                    linked_groundtruth += 1
                    action = "linked_groundtruth"
                else:
                    repaired += 1
                    action = "repaired_groundtruth"
                batch.promoted_video_job_id = target
                details.append({
                    "batch_id": batch.id,
                    "batch_name": batch.name,
                    "video_job_id": target,
                    "action": action,
                    "previous_link": existing_link,
                    "confidence": "high",
                })
            # If existing_link == target, no-op (already correct).
            continue

        # Case B: batch points at a Job that no longer exists — clear
        if existing_link is not None and existing_link not in valid_job_ids:
            batch.promoted_video_job_id = None
            cleared_stale += 1
            details.append({
                "batch_id": batch.id,
                "batch_name": batch.name,
                "video_job_id": None,
                "action": "cleared_stale",
                "previous_link": existing_link,
                "confidence": "high",
            })
            continue

        # Case C: batch points at a Job that DOES exist but the Job's
        # config_json points at a DIFFERENT batch — that means the link
        # is wrong (the Job was actually promoted from a different batch).
        # Clear it; the other batch's ground-truth pass will set itself.
        if existing_link is not None:
            existing_job = next((j for j in all_jobs if j.id == existing_link), None)
            if existing_job is not None and existing_job.id in jobs_claimed_by_groundtruth:
                # Some OTHER batch's config_json claims this job.
                # Don't fight ground truth — clear our wrong link.
                batch.promoted_video_job_id = None
                repaired += 1
                details.append({
                    "batch_id": batch.id,
                    "batch_name": batch.name,
                    "video_job_id": None,
                    "action": "repaired_cleared_wrong",
                    "previous_link": existing_link,
                    "confidence": "high",
                })
                continue

        # Case D: no ground truth available for this batch.
        #
        # v562 had a "Tier 3" proximity fallback here that auto-linked
        # batches to nearby video jobs based on time + assembly_mode
        # signal. That produced wrong links in real-world data because
        # `assembly_mode=True` doesn't prove the job came from THIS
        # batch — only that it came from SOME batch via promote-to-
        # video, and orphan-ground-truth jobs (assembly_mode=True with
        # missing/stale imported_from_batch field) were stolen by
        # unrelated batches.
        #
        # v563 over-corrected by ALSO deleting state-3 links (linked,
        # no ground truth either way) on the assumption they were all
        # wrong. That deleted many correct legacy links — links that
        # were created via older promotion paths that never wrote
        # imported_from_batch.
        #
        # v564: do nothing here. If a batch has no ground truth
        # available, leave it in whatever state it's in. This means:
        #   - Truly unlinked batches stay unlinked. User clicks
        #     "Promote to video" or uses /link-video manually.
        #   - State-3 links (linked, no ground truth) are LEFT ALONE.
        #     They might be correct legacy links from older paths, or
        #     they might be wrong v562 Tier 3 mistakes — we can't tell
        #     without more signal, and clearing them was a mistake in
        #     v563. Better to leave them and let the user fix
        #     individual cases manually if they spot a bad one.
        pass

        # Case E: nothing we can do. Leave the link state alone (whether
        # it was None or a non-conflicting older value).
        if existing_link is None:
            unlinkable += 1

    db.commit()

    # Pre-v562 response shape was {ok, linked, total_unlinked, details}.
    # v562 keeps backward compat by mapping the new counters into 'linked'
    # but also exposes the new categorized counts so the frontend can
    # decide whether to show "auto-fixed wrong links" toasts vs
    # "linked new batches" toasts.
    total_writes = linked_groundtruth + linked_proximity + repaired + cleared_stale
    return {
        "ok": True,
        "linked": total_writes,           # backward-compat
        "total_unlinked": unlinkable,     # backward-compat (now means "still unlinkable")
        # v562 extensions
        "linked_groundtruth": linked_groundtruth,
        "linked_proximity": linked_proximity,
        "repaired": repaired,
        "cleared_stale": cleared_stale,
        "unlinkable": unlinkable,
        "details": details,
    }


@router.post("/batches/{batch_id}/promote-to-video")
def promote_batch_to_video(
    batch_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Take a completed image batch and create a video Job + Clip rows from it.

    Requirements:
      - Every scene node in the batch must be status='ready' with a
        chosen_variant_id set. If anything is missing, return 409 with
        the list of scene nodes that aren't ready.

    What gets built:
      - A new video Job with backend='flow' (or whatever the default is),
        name derived from the batch name, and images_dir pointing to a
        fresh per-job folder populated with the chosen variant files.
      - One Clip per scene, in scene_index order, with:
          * dialogue_text = voiceover_text (or empty string if missing)
          * clip_mode = the parsed clip_mode (blend/fresh) or 'blend' default
          * scene_index = the scene_index_in_batch
          * start_frame = the R2-style key for the copied variant file
          * prompt_text left unset — video worker builds it from action_note
    """
    # 1. Load batch + its scenes
    batch = db.query(ImageJobBatch).filter(
        ImageJobBatch.id == batch_id,
        ImageJobBatch.user_id == current_user.id,
    ).first()
    if not batch:
        raise HTTPException(404, f"Batch {batch_id} not found")

    nodes = db.query(ImageNode).filter(
        ImageNode.batch_id == batch_id,
        ImageNode.user_id == current_user.id,
        ImageNode.kind == "generated",
    ).order_by(ImageNode.scene_index_in_batch).all()

    if not nodes:
        raise HTTPException(400, f"Batch {batch_id} has no scene nodes")

    # 2. Verify every scene is ready + chosen
    missing = []
    for n in nodes:
        if n.status != "ready" or n.chosen_variant_id is None:
            missing.append({
                "node_id": n.id,
                "scene_index": n.scene_index_in_batch,
                "name": n.name,
                "status": n.status,
                "has_chosen_variant": n.chosen_variant_id is not None,
            })
    if missing:
        raise HTTPException(409, {
            "error": "Not all scenes are ready to promote",
            "missing_count": len(missing),
            "total_count": len(nodes),
            "missing": missing,
        })

    # 3. Pre-validate: every chosen variant must have a readable file on disk
    from shutil import copy2
    import uuid as _uuid
    import json as _json
    from models import Job, Clip  # imported locally to avoid circular imports

    # Import app_config for uploads_dir — we follow the same directory
    # convention as main.py's /api/jobs creation path.
    from config import app_config

    new_job_id = str(_uuid.uuid4())
    job_images_dir = app_config.uploads_dir / new_job_id
    job_output_dir = app_config.uploads_dir / new_job_id / "output"
    job_images_dir.mkdir(parents=True, exist_ok=True)
    job_output_dir.mkdir(parents=True, exist_ok=True)

    # 4. Copy each chosen variant file into the new job's images_dir
    #    and build the dialogue + scenes list that Job.dialogue_json needs.
    dialogue_list: List[Dict[str, Any]] = []
    scenes_list: List[Dict[str, Any]] = []
    clip_specs: List[Dict[str, Any]] = []   # collected for post-Job Clip inserts
    # v612 — track R2 keys for frames so the new Job can survive Render
    # ephemeral-filesystem redeploys. Without this, cloning the promoted
    # job, the /api/jobs/{id}/images/{filename} serving endpoint, and
    # the video worker's start_frame fetch all 404 after the next deploy
    # because the local copies are gone and the `jobs/{job_id}/frames/`
    # R2 prefix was never populated.
    frames_storage_keys: Dict[str, str] = {}
    try:
        from backends.storage import is_storage_configured, get_storage as _get_storage
        _r2_configured = is_storage_configured()
        _r2_storage = _get_storage() if _r2_configured else None
    except Exception as _se:
        log.warning(f"[image_platform] R2 storage init failed for promote: {_se}")
        _r2_configured = False
        _r2_storage = None

    # v572 — load all assignments for this batch in one query so we can
    # propagate per-line Veo prompt overrides to dialogue_list. The
    # node carries the denormalized FIRST clip's overrides; the
    # assignment row carries the authoritative full per-line array.
    # Multiple scenes can share an image, but each scene has its own
    # assignment row keyed by scene_index, so this lookup is unambiguous.
    assignments_by_scene_idx: Dict[int, Any] = {}
    try:
        for _a in db.query(ImageSceneAssignment).filter(
            ImageSceneAssignment.batch_id == batch_id
        ).order_by(ImageSceneAssignment.scene_index).all():
            assignments_by_scene_idx[_a.scene_index] = _a
    except Exception as _ae:
        log.warning(f"[image_platform] could not load assignments for batch {batch_id}: {_ae}")

    for idx, n in enumerate(nodes):
        variant = db.query(ImageVariant).filter(ImageVariant.id == n.chosen_variant_id).first()
        if not variant:
            raise HTTPException(500, f"Node {n.id}: chosen variant {n.chosen_variant_id} missing from DB")

        src_path = images_root() / variant.image_path
        if not src_path.exists():
            # v477: R2 rehydration fallback before failing.
            log.info(f"[image_platform] Variant file missing locally, attempting R2 restore: {variant.image_path}")
            if not _storage_download_to_local(variant.image_path):
                raise HTTPException(500,
                    f"Node {n.id}: variant file missing at {src_path} and not recoverable from R2")
            if not src_path.exists():
                raise HTTPException(500,
                    f"Node {n.id}: R2 reported success but file still missing at {src_path}")

        # Use a deterministic filename the video pipeline expects:
        # image_{idx:02d}{ext} — the existing prompt-only / scene flows
        # use this convention (start_frame -> filename in images_dir).
        ext = src_path.suffix or ".png"
        dst_filename = f"image_{idx:02d}{ext}"
        dst_path = job_images_dir / dst_filename
        try:
            copy2(src_path, dst_path)
        except Exception as e:
            raise HTTPException(500, f"Node {n.id}: failed to copy variant file: {e}")

        # v612 — mirror to R2 at the canonical jobs/{job_id}/frames/{filename}
        # prefix so /api/jobs/{job_id}/images/{filename}, the clone-job
        # config endpoint, and the video worker's start_frame fetch all
        # work after a Render redeploy. The standard /api/jobs upload
        # path goes through a background task that does this same upload
        # (main.py ~line 1936); we replicate that step inline here so
        # promoted jobs reach the same persistence baseline.
        if _r2_configured and _r2_storage is not None:
            try:
                _r2_storage.upload_job_frame(new_job_id, dst_filename, dst_path)
                frames_storage_keys[dst_filename] = f"jobs/{new_job_id}/frames/{dst_filename}"
            except Exception as _ue:
                log.warning(
                    f"[image_platform] v612: R2 upload of frame {dst_filename} "
                    f"failed for new job {new_job_id}: {_ue}"
                )

        line_text_default = n.voiceover_text or ""
        mode = (n.clip_mode or "blend").lower()
        if mode not in ("blend", "fresh", "continue"):
            mode = "blend"

        # v572.1 — multi-line scene expansion at promote time.
        # The existing pre-v572 promote flow iterated nodes and emitted
        # ONE dialogue entry per node, taking n.voiceover_text (the
        # denormalized FIRST line). That silently dropped every line
        # past index 0 in multi-line scenes — the lines_json /
        # action_notes_json / veo_prompts_json arrays on the assignment
        # row carried the authoritative full data but were never read.
        #
        # Now: read the assignment row and emit one dialogue entry per
        # line. All entries for the same node share start_frame_key
        # (same underlying image) and scene_index. Each entry carries
        # its own line text, action_note, and Veo prompt override
        # pulled from the parallel arrays.
        _assignment = assignments_by_scene_idx.get(n.scene_index_in_batch)
        if _assignment:
            _ad = _assignment.to_dict()
            scene_lines = _ad.get("lines") or []
            scene_notes = _ad.get("action_notes") or []
            scene_veo_prompts = _ad.get("veo_prompts") or []
            if not scene_lines:
                # Empty assignment (shouldn't happen) — fall back to denorm
                scene_lines = [line_text_default]
                scene_notes = [n.action_note or ""]
                scene_veo_prompts = [None]
        else:
            # Truly legacy data predating ImageSceneAssignment — use the
            # node's denormalized fields. Only one line per node here.
            scene_lines = [line_text_default]
            scene_notes = [n.action_note or ""]
            if n.veo_prompt_override:
                scene_veo_prompts = [{
                    "text_prompt": n.veo_prompt_override,
                    "negative_prompt": n.veo_negative_prompt_override,
                }]
            else:
                scene_veo_prompts = [None]

        # All clips in this scene share the same start frame.
        start_frame_key = f"jobs/{new_job_id}/frames/{dst_filename}"

        # Track which clip indices belong to this scene — main.py's
        # blend-mode end_frame logic reads scenes_list[N]["clips"]
        # to find scene-boundary cuts.
        clips_in_this_scene: List[int] = []

        for line_pos in range(len(scene_lines)):
            line_text_i = scene_lines[line_pos] or ""
            note_i = scene_notes[line_pos] if line_pos < len(scene_notes) else None
            vp_i = scene_veo_prompts[line_pos] if line_pos < len(scene_veo_prompts) else None

            _veo_text_override = (vp_i or {}).get("text_prompt") if vp_i else None
            _veo_neg_override = (vp_i or {}).get("negative_prompt") if vp_i else None

            current_clip_index = len(dialogue_list)  # 0-based position

            dialogue_list.append({
                "id": current_clip_index + 1,
                "text": line_text_i,
                "clip_mode": mode,
                "scene_index": idx,
                # All lines in this scene use this scene's image as start frame.
                # main.py reads start_image_idx to pick from uploaded_frames_list.
                "start_image_idx": idx,
                # Extra metadata we pass through so the video worker /
                # prompt-builder can use it when composing the Veo prompt:
                "action_note": note_i or "",
                "visual_register": n.visual_register or "",
                "rhythm_tier": n.rhythm_tier or "",
                "scene_transition": n.scene_transition or "",
                # v537 — explicit speaker mode declared in markdown.
                # main.py reads this and converts to voiceover_only flag.
                "speaker_mode": n.speaker_mode or "",
                # v572 — per-clip Veo prompt overrides. When non-empty,
                # build_prompt is bypassed and these are shipped verbatim
                # (with negative-prompt trailer concatenated). Empty/null =
                # auto-build runs as before.
                "veo_prompt_override": _veo_text_override or None,
                "veo_negative_prompt_override": _veo_neg_override or None,
            })

            clip_specs.append({
                "clip_index": current_clip_index,
                "dialogue_id": current_clip_index + 1,
                "dialogue_text": line_text_i,
                "clip_mode": mode,
                "scene_index": idx,
                "start_frame": start_frame_key,
                "end_frame": None,  # filled below for blend mode
                "scene_transition": n.scene_transition or "",
            })

            clips_in_this_scene.append(current_clip_index)

        scenes_list.append({
            "scene_index": idx,
            "image_filename": dst_filename,
            # main.py's blend-mode end_frame logic reads camelCase keys
            # — provide both for compat.
            "imageIndex": idx,
            "action_note": (scene_notes[0] if scene_notes else "") or "",
            "visual_register": n.visual_register or "",
            "clip_mode": mode,
            "scene_transition": n.scene_transition or "",
            "transition": n.scene_transition or "",
            # v572.1 — main.py expects scenes[N]["clips"] = [list of clip
            # indices in this scene]. Without this, blend-mode end_frame
            # logic silently no-ops and within-scene transitions can't
            # resolve. With multi-line expansion, this field is required.
            "clips": clips_in_this_scene,
        })

    # Blend mode: if scene N is blend and scene N+1 exists, the Veo
    # generation uses scene N+1's first frame as the last_frame.
    for i, spec in enumerate(clip_specs):
        if spec["clip_mode"] == "blend" and i + 1 < len(clip_specs):
            spec["end_frame"] = clip_specs[i + 1]["start_frame"]

    # 5. Create the Job row
    #    We follow main.py's existing convention: dialogue_json holds
    #    {lines, scenes}, config_json holds language/duration/etc.
    job_name = batch.name or "Imported from batch"
    config_dict = {
        "language": "English",
        "duration": "8",
        "imported_from_batch": batch_id,
        "imported_from_batch_name": job_name,
        "assembly_mode": True,
        # v612 — promoted jobs ARE storyboard-mode (have scenes data, multi-
        # image lineup, scene-line associations). Without this flag set, the
        # /api/jobs/{id}/config-driven cloneJob frontend (static/index.html
        # ~line 10754) discards data.scenes and falls into 'auto' editor
        # mode, losing the multi-line scene structure and image-line
        # bindings. Setting it True keeps the storyboard layout on clone.
        "storyboard_mode": True,
    }
    if batch.persona:
        config_dict["persona"] = batch.persona
    if batch.setting:
        config_dict["setting"] = batch.setting

    job = Job(
        id=new_job_id,
        user_id=current_user.id,  # v447: image platform now has user context, propagate to video Job
        status="pending",
        config_json=_json.dumps(config_dict),
        dialogue_json=_json.dumps({
            "lines": dialogue_list,
            "scenes": scenes_list,
            "last_frame_index": len(nodes) - 1 if nodes else 0,
        }),
        images_dir=str(job_images_dir),
        output_dir=str(job_output_dir),
        total_clips=len(clip_specs),
        backend="flow",
        # v612 — mirror frames to R2 at canonical jobs/{id}/frames/{filename}
        # prefix during the file-copy loop above, then stamp the keys map
        # here so the video worker, image-serving endpoint, and clone-job
        # config endpoint can all rehydrate from R2 after Render redeploy.
        frames_storage_keys=_json.dumps(frames_storage_keys) if frames_storage_keys else None,
    )
    db.add(job)
    db.flush()
    if frames_storage_keys:
        log.info(
            f"[image_platform] v612: promoted job {new_job_id[:8]} mirrored "
            f"{len(frames_storage_keys)} frame(s) to R2"
        )
    elif _r2_configured:
        log.warning(
            f"[image_platform] v612: promoted job {new_job_id[:8]} — R2 "
            f"configured but no frames uploaded successfully (clone will "
            f"break after Render redeploy)"
        )

    # v575: import the prompt-composer so Clips with prebuilt overrides get
    # their final Veo prompt stamped onto Clip.prompt_text at promote time.
    # Without this, the override was carried in Job.dialogue_json but the
    # per-clip prompt_text stayed NULL — and since _setup_job_background
    # (where overrides normally get applied) is only spawned by /api/jobs
    # and NOT by /promote-to-video, the Flow worker's "use clip.prompt if
    # set, else rebuild from scratch" check at flow_worker.py:11008-11019
    # always fell into the rebuild branch, silently bypassing the
    # markdown's `## Veo 3.1 Final Prompts (per clip)` section.
    try:
        from veo_prompt_overrides import compose_final_prompt as _compose_veo_prompt
    except ImportError:
        _compose_veo_prompt = None

    for spec in clip_specs:
        # v575: look up the parallel dialogue entry to find any v572
        # prebuilt prompt override. dialogue_list and clip_specs are
        # built in lockstep above (one append to each per line), so
        # dialogue_list[spec["clip_index"]] always exists when clip_specs
        # has the same index.
        _clip_idx = spec["clip_index"]
        _matching_dialogue = (
            dialogue_list[_clip_idx]
            if _clip_idx < len(dialogue_list) else None
        )
        _veo_override = (_matching_dialogue or {}).get("veo_prompt_override") if _matching_dialogue else None
        _veo_neg = (_matching_dialogue or {}).get("veo_negative_prompt_override") if _matching_dialogue else None
        # When a prebuilt override exists, compose the final Veo prompt
        # (text + optional Negative prompt trailer) and stamp it onto
        # Clip.prompt_text. The Flow worker's clip.get('prompt') check
        # then finds it and ships it verbatim. When no override exists,
        # leave prompt_text NULL — the existing fallback at
        # flow_worker.py:11010 builds the prompt the legacy way.
        _prompt_text = None
        if _veo_override and _compose_veo_prompt is not None:
            _prompt_text = _compose_veo_prompt(_veo_override, _veo_neg)

        clip = Clip(
            job_id=new_job_id,
            clip_index=spec["clip_index"],
            dialogue_id=spec["dialogue_id"],
            dialogue_text=spec["dialogue_text"],
            status="pending",
            clip_mode=spec["clip_mode"],
            scene_index=spec["scene_index"],
            start_frame=spec["start_frame"],
            end_frame=spec["end_frame"],
            prompt_text=_prompt_text,  # v575 — set when prebuilt override exists
        )
        db.add(clip)

    # 6. Remember on the batch that we've already promoted it
    batch.promoted_video_job_id = new_job_id
    db.commit()

    return {
        "video_job_id": new_job_id,
        "batch_id": batch_id,
        "total_clips": len(clip_specs),
        "job_name": job_name,
    }


def _promote_ready_children(db: Session, parent_node_id: int):
    """Check all children of parent_node_id. For each child currently in
    'draft', if ALL its parents are ready+chosen, promote it to 'queued'
    and write its job file.

    Called after a node becomes ready with a chosen variant.

    v511: rewrote to eager-load child + grandparent rows in two batched
    queries instead of one-per-child + one-per-grandparent-edge. Previous
    implementation triggered N+1 lazy loads while holding the DB session,
    contributing to pool exhaustion when a Salvora-style import promoted
    many scene nodes through a chain of bootstrap parents.
    """
    # Step 1 — find immediate child IDs in one query
    child_ids = [
        cid for (cid,) in db.query(ImageEdge.child_node_id).filter(
            ImageEdge.parent_node_id == parent_node_id
        ).distinct().all()
    ]
    if not child_ids:
        return

    # Step 2 — load each child WITH its parent_edges and grandparent nodes
    # eagerly so the inner loop doesn't issue per-row SELECTs.
    children = db.query(ImageNode).filter(
        ImageNode.id.in_(child_ids),
        ImageNode.status == "draft",
    ).options(
        joinedload(ImageNode.parent_edges).joinedload(ImageEdge.parent)
    ).all()
    if not children:
        return

    promoted = 0
    for child in children:
        # All parents must be ready+chosen. parent_edges and edge.parent
        # are already loaded by the joinedload above.
        all_parents_ready = True
        for pe in child.parent_edges:
            parent = pe.parent
            if parent is None or parent.status != "ready" or parent.chosen_variant_id is None:
                all_parents_ready = False
                break
        if not all_parents_ready:
            continue

        try:
            child.status = "queued"
            child.error_message = None
            db.flush()
            write_generation_job(db, child)
            promoted += 1
            log.info(f"[image_platform] Auto-promoted child node {child.id} (parent {parent_node_id} became ready)")
        except Exception as e:
            log.error(f"[image_platform] Failed to promote child {child.id}: {e}")
            child.status = "draft"
            child.error_message = f"Queue failed: {e}"

    if promoted:
        db.commit()


# ---- Worker distribution (serve the worker script to the user) -----------
# The same pattern as /api/user-worker/download/flow_worker.py for video:
# the user's machine pulls the worker script directly from the webapp, so
# they don't need to clone the whole repo.

@router.get("/worker/download/image_worker.py")
def serve_image_worker_script():
    """Serve the latest image_worker.py source code.
    Called by the PowerShell setup script (and directly if the user wants)."""
    from fastapi.responses import Response as FAResponse
    worker_path = Path(__file__).parent / "image_worker.py"
    if not worker_path.exists():
        raise HTTPException(404, "Worker script not found on server")
    return FAResponse(content=worker_path.read_text(encoding="utf-8"),
                      media_type="text/x-python")


@router.get("/worker/download/setup.ps1")
def serve_image_worker_setup_ps1(request: Request, api_key: Optional[str] = None):
    """Serve a self-contained PowerShell script that:
      1. Finds/installs Python
      2. Installs patchright + requests if needed
      3. Downloads image_worker.py from this server
      4. Launches it with --api-url pointing to this server

    Usage from Windows terminal:
        powershell -c "irm <app>/api/images/worker/download/setup.ps1 | iex"

    Optionally the api_key can be prefilled via ?api_key=... (URL is
    built dynamically by the UI).
    """
    from fastapi.responses import Response as FAResponse

    # Use env var default if user didn't pass one via query string
    default_key = api_key or _get_worker_api_key()
    app_url = str(request.base_url).rstrip("/")

    script = r"""# KavenoBuilder Image Worker - Quick Setup (Windows)
# Usage: powershell -c "irm __APP_URL__/api/images/worker/download/setup.ps1 | iex"

$ErrorActionPreference = 'Continue'
$AppUrl  = '__APP_URL__'
$ApiKey  = '__API_KEY__'
$WorkDir = "$env:USERPROFILE\KavenoImageWorker"

Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "  KavenoBuilder Image Worker - Quick Setup"     -ForegroundColor Cyan
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Working directory: $WorkDir"
Write-Host ""

# Ensure workdir exists
if (-not (Test-Path $WorkDir)) {
    New-Item -ItemType Directory -Path $WorkDir -Force | Out-Null
}

# Find Windows Python (skip MSYS2/Cygwin/MinGW)
function Find-WindowsPython {
    $badPaths = @("msys", "cygwin", "mingw", "ucrt64", "clang64")
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        $pyPath = $py.Source
        $lower = $pyPath.ToLower()
        $isBad = $false
        foreach ($bad in $badPaths) { if ($lower -like "*$bad*") { $isBad = $true } }
        if (-not $isBad) {
            $pipTest = & $pyPath -m pip --version 2>&1
            if ($LASTEXITCODE -eq 0) { return $pyPath }
        }
    }
    foreach ($name in @("python", "python3")) {
        $cmds = Get-Command $name -ErrorAction SilentlyContinue -All
        if ($cmds) {
            foreach ($cmd in $cmds) {
                $p = $cmd.Source
                $lower = $p.ToLower().Replace("\", "/")
                $isBad = $false
                foreach ($bad in $badPaths) { if ($lower -like "*$bad*") { $isBad = $true } }
                if ($isBad) { continue }
                $pipTest = & $p -m pip --version 2>&1
                if ($LASTEXITCODE -eq 0) { return $p }
            }
        }
    }
    return $null
}

Write-Host "Looking for Python..."
$pythonPath = Find-WindowsPython
if (-not $pythonPath) {
    Write-Host "No Python found. Install from https://python.org/downloads" -ForegroundColor Red
    Write-Host "Make sure to check 'Add Python to PATH' during installation." -ForegroundColor Yellow
    exit 1
}
# Resolve py.exe to actual interpreter
if ($pythonPath -like "*\py.EXE" -or $pythonPath -like "*\py.exe") {
    $resolved = & $pythonPath -c "import sys; print(sys.executable)" 2>$null
    if ($resolved -and (Test-Path $resolved)) { $pythonPath = $resolved }
}
Write-Host "Python: $pythonPath" -ForegroundColor Green

# Install deps if missing
Write-Host ""
Write-Host "Checking packages (patchright, requests)..."
$check = & $pythonPath -c "import patchright; import requests; print('ok')" 2>$null
if ($check -ne "ok") {
    Write-Host "Installing patchright + requests (first run, 1-3 min)..." -ForegroundColor Yellow
    Start-Process -FilePath $pythonPath -ArgumentList "-m","pip","install","--no-input","patchright","requests" -NoNewWindow -Wait
    # Playwright/Patchright also needs Chrome binaries — run install if not already done
    Start-Process -FilePath $pythonPath -ArgumentList "-m","patchright","install","chrome" -NoNewWindow -Wait
}

# Download the worker script (always fresh — ensures updates propagate)
Write-Host ""
Write-Host "Downloading image_worker.py..."
$workerPath = Join-Path $WorkDir "image_worker.py"
Invoke-WebRequest -Uri "$AppUrl/api/images/worker/download/image_worker.py" -OutFile $workerPath -UseBasicParsing
Write-Host "Saved to: $workerPath" -ForegroundColor Green

# Launch the worker
Write-Host ""
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "  Starting worker..."                            -ForegroundColor Cyan
Write-Host "  Chrome will open. Login to Google Flow once." -ForegroundColor Cyan
Write-Host "  Keep this window open while generating!"      -ForegroundColor Cyan
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host ""

Set-Location $WorkDir
& $pythonPath image_worker.py --api-url $AppUrl --api-key $ApiKey
"""

    # Substitute placeholders
    script = script.replace("__API_KEY__", default_key).replace("__APP_URL__", app_url)
    return FAResponse(content=script, media_type="text/plain")


# v492: downloadable installer mirroring the video worker pattern
# (my-worker.html + /api/user-worker/download/installer). Users get a
# .bat on Windows or a .command-in-.zip on Mac/Linux, with a reset
# checkbox to wipe Chrome sessions. The existing inline /setup.ps1
# endpoint above stays as a fallback for manual use.


@router.get("/worker/download/installer")
def download_image_worker_installer(
    request: Request,
    os: str = Query("windows", regex="^(windows|mac|linux)$"),
    reset: int = Query(0),
    update_only: int = Query(0),
    parallel: int = Query(2, ge=1, le=8),
    api_key: Optional[str] = None,
):
    """Generate a downloadable installer for the image worker.

    Windows → .bat file
    Mac/Linux → .command inside a .zip (preserves exec bit)

    Query params:
      os: 'windows' | 'mac' | 'linux'
      reset=1: wipe session folders before launch (force re-login)
      update_only=1: only re-download image_worker.py, don't launch
      parallel: concurrent in-flight generations (default 2, range 1-8).
                Higher = more throughput per Flow session, but Flow's web
                UI gets sluggish past 4-5 in-flight tiles per page (DOM
                grows, scroll-extract slows). v571: was hardcoded to the
                worker's CLI default (2); now user-selectable in the UI
                before download.
    """
    from fastapi.responses import Response as FAResponse

    # Bake the worker API key into the installer so users don't have to
    # paste it. Same env-var lookup as the inline setup.ps1.
    effective_key = api_key or _get_worker_api_key()
    app_url = str(request.base_url).rstrip("/")
    if "kavenobuilder.com" not in app_url and "localhost" not in app_url and "127.0.0.1" not in app_url:
        app_url = "https://kavenobuilder.com"

    if os == "windows":
        content = _generate_image_windows_installer(
            effective_key, app_url, bool(reset), bool(update_only),
            parallel_slots=parallel)
        return FAResponse(
            content=content,
            media_type="application/x-bat",
            headers={
                "Content-Disposition": "attachment; filename=KavenoImageWorker-Setup.bat",
                "Cache-Control": "no-store",
            },
        )
    else:
        # Mac / Linux: wrap .command in .zip so exec bit survives the
        # browser download. Mirrors the video worker's approach exactly.
        import zipfile
        import io
        content = _generate_image_unix_installer(
            effective_key, app_url, bool(reset), bool(update_only),
            parallel_slots=parallel)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            info = zipfile.ZipInfo("KavenoImageWorker-Setup.command")
            info.external_attr = 0o755 << 16  # rwxr-xr-x
            info.create_system = 3  # Unix
            zf.writestr(info, content)
        return FAResponse(
            content=buf.getvalue(),
            media_type="application/zip",
            headers={
                "Content-Disposition": "attachment; filename=KavenoImageWorker-Setup.zip",
                "Cache-Control": "no-store",
            },
        )


def _generate_image_windows_installer(
    api_key: str, app_url: str, reset: bool = False, update_only: bool = False,
    parallel_slots: int = 2,
) -> str:
    """Windows .bat installer. Mirrors video worker installer structure.

    Folder layout:
      %USERPROFILE%\\KavenoImageWorker\\
        image_worker.py
        image-chrome-session\\
        image-chrome-golden\\      (created by worker on first complete run)
    """
    # Reset block — wipe Chrome sessions + golden profile.
    # Note: wipes numbered variants too so future multi-account support
    # doesn't leave stale data around, even though we don't use them yet.
    reset_cmds = ""
    if reset:
        reset_cmds = (
            'echo   Resetting sessions...\n'
            'if exist "%WORKER_DIR%\\image-chrome-session" rmdir /s /q "%WORKER_DIR%\\image-chrome-session" 2>nul\n'
            'if exist "%WORKER_DIR%\\image-chrome-golden" rmdir /s /q "%WORKER_DIR%\\image-chrome-golden" 2>nul\n'
        )
        for n in range(2, 5):
            reset_cmds += f'if exist "%WORKER_DIR%\\image-chrome-session-{n}" rmdir /s /q "%WORKER_DIR%\\image-chrome-session-{n}" 2>nul\n'
            reset_cmds += f'if exist "%WORKER_DIR%\\image-chrome-golden-{n}" rmdir /s /q "%WORKER_DIR%\\image-chrome-golden-{n}" 2>nul\n'
        reset_cmds += 'echo   [OK] Sessions reset\necho.\n'

    if update_only:
        return f'''@echo off
setlocal enabledelayedexpansion
title KavenoBuilder Image Worker
mode con: cols=56 lines=14
color 1F
echo.
echo   KavenoBuilder Image Worker - Update
echo.
set "WORKER_DIR=%USERPROFILE%\\KavenoImageWorker"
if not exist "%WORKER_DIR%" mkdir "%WORKER_DIR%"
echo   Downloading latest image_worker.py...
powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '{app_url}/api/images/worker/download/image_worker.py' -OutFile '%WORKER_DIR%\\image_worker.py' -UseBasicParsing" >nul 2>nul
echo   [OK] Updated
echo.
echo   Restart your worker to use the new version.
timeout /t 4 /nobreak >nul
exit
'''

    return f'''@echo off
setlocal enabledelayedexpansion
title KavenoBuilder Image Worker
mode con: cols=64 lines=28
color 1F

echo.
echo   ======================================================
echo    KavenoBuilder Image Worker Setup
echo   ======================================================
echo.

set "API_KEY={api_key}"
set "APP_URL={app_url}"
set "WORKER_DIR=%USERPROFILE%\\KavenoImageWorker"
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
echo         Not found - installing via winget...
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

echo         Installing Chrome for Patchright (first time only, ~200MB)...
!PY! -m patchright install chrome >nul 2>nul
echo         OK

echo   [3/5] Downloading worker...
if not exist "%WORKER_DIR%" mkdir "%WORKER_DIR%"
mkdir "%WORKER_DIR%\\image-chrome-session" 2>nul
powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '%APP_URL%/api/images/worker/download/image_worker.py' -OutFile '%WORKER_DIR%\\image_worker.py' -UseBasicParsing" >nul 2>nul
echo         OK

echo   [4/5] Writing config...
(
echo IMAGE_SESSION_FOLDER=%WORKER_DIR%\\image-chrome-session
echo LOCAL_WORKER_API_KEY=%API_KEY%
echo PARALLEL_SLOTS={parallel_slots}
) > "%WORKER_DIR%\\.env"
echo         OK

echo   [5/5] Ready.
echo.
echo   ======================================================
echo    Setup complete! Starting worker...
echo   ======================================================
echo.
echo   Chrome will open minimized. Log in to Google Flow ONCE
echo   if prompted, then close nothing - keep this window open.
echo   Check status: %APP_URL%/
echo.

cd /d "%WORKER_DIR%"
for /f "usebackq tokens=1,* delims==" %%a in ("%WORKER_DIR%\\.env") do set "%%a=%%b"
!PY! image_worker.py --api-url %APP_URL% --api-key %API_KEY% --session "%WORKER_DIR%\\image-chrome-session" --parallel %PARALLEL_SLOTS%

echo.
echo   Worker stopped. Press any key to close.
pause >nul
'''


def _generate_image_unix_installer(
    api_key: str, app_url: str, reset: bool = False, update_only: bool = False,
    parallel_slots: int = 2,
) -> str:
    """Mac/Linux .command installer. Mirrors video worker installer."""
    reset_cmds = ""
    if reset:
        reset_cmds = (
            '\necho "  Resetting sessions..."\n'
            'rm -rf "$DIR/image-chrome-session" "$DIR/image-chrome-golden"\n'
        )
        for n in range(2, 5):
            reset_cmds += f'rm -rf "$DIR/image-chrome-session-{n}" "$DIR/image-chrome-golden-{n}"\n'
        reset_cmds += 'echo "  [OK] Sessions reset"\n'

    if update_only:
        return f'''#!/bin/bash
DIR="$HOME/KavenoImageWorker"
mkdir -p "$DIR"
echo ""
echo "  Updating KavenoBuilder Image Worker..."
curl -sL "{app_url}/api/images/worker/download/image_worker.py" -o "$DIR/image_worker.py"
echo "  [OK] Updated. Restart your worker to use the new version."
echo ""
sleep 3
'''

    return f'''#!/bin/bash
# KavenoBuilder Image Worker Setup

set -e
DIR="$HOME/KavenoImageWorker"
LOG="$DIR/setup.log"
mkdir -p "$DIR"

log() {{ echo "$(date +%H:%M:%S) $1" >> "$LOG"; }}

echo ""
echo "  ======================================================"
echo "   KavenoBuilder Image Worker Setup"
echo "  ======================================================"
echo ""

log "=== KavenoBuilder Image Worker Setup ==="

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
echo "  [2/5] Installing packages..."
$PY -m pip install patchright requests --quiet 2>/dev/null || \\
$PY -m pip install patchright requests --quiet --user 2>/dev/null || \\
$PY -m pip install patchright requests --quiet --break-system-packages 2>/dev/null || true
log "Packages installed"

echo "        Installing Chrome for Patchright (first time only, ~200MB)..."
$PY -m patchright install chrome >> "$LOG" 2>&1 || true
echo "        OK"

echo "  [3/5] Downloading worker..."
curl -sL "{app_url}/api/images/worker/download/image_worker.py" -o "$DIR/image_worker.py"
mkdir -p "$DIR/image-chrome-session"
log "Worker downloaded"
echo "        OK"

echo "  [4/5] Writing config..."
cat > "$DIR/.env" << ENVEOF
IMAGE_SESSION_FOLDER=$DIR/image-chrome-session
LOCAL_WORKER_API_KEY={api_key}
PARALLEL_SLOTS={parallel_slots}
ENVEOF
log ".env written"
echo "        OK"

echo "  [5/5] Ready."
echo ""
echo "  ======================================================"
echo "   Setup complete! Starting worker..."
echo "  ======================================================"
echo ""

log "Starting worker"

cd "$DIR"
set -a; source .env; set +a
echo "  Chrome will open - log into Google Flow once if prompted,"
echo "  then keep this window open while the worker runs."
echo "  Check status: {app_url}/"
echo ""
$PY image_worker.py --api-url "{app_url}" --api-key "{api_key}" --session "$DIR/image-chrome-session" --parallel "$PARALLEL_SLOTS"

echo ""
echo "  Worker stopped. Press Enter to close."
read
'''


# ---- worker status -------------------------------------------------------

# How fresh must `_worker_ready` be for the worker to be considered "online".
# The worker writes this file once at startup; to detect crashes we also
# bump its mtime from the polling loop (see watch_folder_mode patch).
WORKER_HEARTBEAT_STALE_SECONDS = 20  # v545: loosened from 10s. The 10s
# threshold combined with v518's 5s write throttle and the worker's 5s
# heartbeat-send interval was producing UI flicker — a single slow
# heartbeat HTTP request (Render DB blip, network jitter, SSL EOF
# recovery) made the DB row 11-15s old, which fell past the 10s
# threshold for one poll cycle, then recovered on the next. The user
# saw "● Online → ● Offline → ● Online → ● Offline" cycling every
# few seconds. With 20s the worker has 4× the throttle write cadence
# of safety margin: even if 2 consecutive heartbeat HTTP requests
# fail, the indicator still stays green. A genuinely-dead worker is
# still detected within 20 seconds, which is well under the time it
# would take a user to notice and care about a stuck job.
# Worker polls every 3.0s when idle, so 10s tolerates one missed poll
# cycle but flags offline by the second missed one. Previous 30s value
# meant the "● Online" indicator stayed green for up to 30 seconds
# after a Ctrl+C kill, giving a misleading UX. The graceful-shutdown
# path (POST /worker/release-claims with going_offline=true) instantly
# clears the heartbeat row so well-behaved shutdowns flip the indicator
# within one UI poll cycle (~2s).


@router.get("/worker/status")
def worker_status(
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Report queue counts, watch-folder path, and whether the external
    image_worker.py process appears to be running (via heartbeat file).

    The queue counts shown here are scoped to the current user — the
    counters drive the Images-tab status strip, which should match what
    the user actually has, not a global worker-wide tally.
    """
    import time as _time

    n_queued = db.query(ImageNode).filter(
        ImageNode.user_id == current_user.id,
        ImageNode.status == "queued",
    ).count()
    n_generating = db.query(ImageNode).filter(
        ImageNode.user_id == current_user.id,
        ImageNode.status == "generating",
    ).count()
    n_ready = db.query(ImageNode).filter(
        ImageNode.user_id == current_user.id,
        ImageNode.status == "ready",
    ).count()
    n_failed = db.query(ImageNode).filter(
        ImageNode.user_id == current_user.id,
        ImageNode.status == "failed",
    ).count()

    # Files currently in the watch folder (queued but not yet ack'd by worker)
    watch = jobs_watch_dir()
    pending_files = (
        len(list(watch.glob("node_*.json")))
        - len(list(watch.glob("node_*.done.json")))
    )

    # Heartbeat: the worker creates/updates `_worker_ready` in the watch folder
    heartbeat_file = watch / "_worker_ready"
    worker_online = False
    heartbeat_age = None
    if heartbeat_file.exists():
        try:
            age = _time.time() - heartbeat_file.stat().st_mtime
            heartbeat_age = round(age, 1)
            worker_online = age < WORKER_HEARTBEAT_STALE_SECONDS
        except Exception:
            pass

    return {
        "queued": n_queued,
        "generating": n_generating,
        "ready": n_ready,
        "failed": n_failed,
        "watch_folder_pending": max(0, pending_files),
        "watch_folder": str(watch),
        "worker_online": worker_online,
        "worker_heartbeat_age_seconds": heartbeat_age,
        # HTTP-pull mode heartbeat (separate from filesystem one)
        "http_worker_online": _worker_http_is_online(db),
    }


# =============================================================================
# HTTP-pull worker API
# =============================================================================
# A remote worker (on a different machine than the webapp) polls these
# endpoints over HTTP. Pattern mirrors /api/local-worker/* for videos:
# Bearer token auth, atomic claim of pending jobs, POST status + upload
# variant files.

from fastapi.responses import Response as FAResponse


def _get_worker_api_key() -> str:
    """Share the same key as the video local-worker so users set it once."""
    return os.environ.get("LOCAL_WORKER_API_KEY", "local-worker-secret-key-12345")


def _verify_worker_key(authorization: Optional[str]):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing Authorization header")
    token = authorization[7:]
    if token != _get_worker_api_key():
        raise HTTPException(401, "Invalid API key")
    return True


def _touch_worker_heartbeat(db: Session, worker_id: Optional[str]):
    """Upsert the last-heartbeat timestamp for this worker in the DB.
    DB-backed state is process-agnostic — works correctly whether the
    webapp runs as a single uvicorn process or under gunicorn with N
    worker processes.

    v518/v545: throttled last-heartbeat writes. Workers poll
    /worker/heartbeat every 5 seconds when idle, which previously meant
    a commit per poll. Throttling reduces DB write load while keeping
    the row fresh enough for the online indicator.

    v545: throttle window 5s → 4s. The previous 5s window matched the
    worker's send interval exactly, so timing jitter routinely caused
    every other heartbeat to skip the write — leaving the DB row
    sometimes 10s+ old, right at the stale-window edge. With 4s, the
    worker's 5s sends are reliably accepted and the row is updated at
    the worker's actual cadence.
    """
    wid = worker_id or "default"
    row = db.query(ImageWorkerHeartbeat).filter(
        ImageWorkerHeartbeat.worker_id == wid
    ).first()
    now = datetime.utcnow()
    if row is None:
        row = ImageWorkerHeartbeat(worker_id=wid, last_heartbeat_at=now)
        db.add(row)
        db.commit()
        return
    # v545: 4s throttle (was 5s). Worker sends every 5s, so any value
    # below 5 ensures the write is accepted on every send. 4s gives a
    # 1s safety margin for jitter without reducing the DB write rate.
    age = (now - row.last_heartbeat_at).total_seconds() if row.last_heartbeat_at else 999
    if age < 4.0:
        return  # skip — recent enough
    row.last_heartbeat_at = now
    db.commit()


def _worker_http_is_online(db: Session) -> bool:
    """Any heartbeat row within the stale window counts as online."""
    cutoff = datetime.utcnow() - timedelta(seconds=WORKER_HEARTBEAT_STALE_SECONDS)
    fresh = db.query(ImageWorkerHeartbeat).filter(
        ImageWorkerHeartbeat.last_heartbeat_at >= cutoff
    ).first()
    return fresh is not None


@router.get("/worker/health")
def worker_health():
    """Cheap ping — no auth, used by worker to check URL reachability."""
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@router.post("/worker/heartbeat")
def worker_heartbeat(
    worker_id: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db_session),
):
    """Worker pings this on every poll cycle. Counted toward 'online'
    indicator in the UI. Persisted in DB so multi-process webapp
    deployments see a consistent online status."""
    _verify_worker_key(authorization)
    _touch_worker_heartbeat(db, worker_id)
    return {"ok": True}


@router.post("/worker/release-claims")
def worker_release_claims(
    worker_id: Optional[str] = None,
    going_offline: bool = False,
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db_session),
):
    """Release all 'generating' claims owned by this worker_id.

    Called by the worker at startup so that any nodes left in
    'generating' status from a previous crashed run are instantly
    re-queued, without waiting for the 10-minute TTL sweep. Without
    this, restarting the worker meant those nodes stayed stuck until
    the sweep fired on a later /jobs/pending poll.

    Scope: only touches nodes whose claimed_by_worker matches the
    given worker_id AND whose status is 'generating'. Safe to call
    repeatedly — if the worker has no stale claims it's a no-op.

    v516: when ``going_offline=True`` is passed (the worker's Ctrl+C
    shutdown handler hits this), the worker's heartbeat row is also
    deleted so the UI flips to "● Offline" within the next poll cycle
    instead of waiting for the 10s stale window. Combined with the
    tightened ``WORKER_HEARTBEAT_STALE_SECONDS = 10`` constant, this
    gives the indicator near-real-time accuracy: well-behaved
    shutdowns flip immediately, crashes flip within ~10s.
    """
    _verify_worker_key(authorization)
    if not worker_id:
        raise HTTPException(400, "worker_id required")

    stale_own = db.query(ImageNode).filter(
        ImageNode.status == "generating",
        ImageNode.claimed_by_worker == worker_id,
    ).all()
    n = len(stale_own)
    for node in stale_own:
        node.status = "queued"
        node.claimed_by_worker = None
        node.claimed_at = None
        node.error_message = None

    n_heartbeats_deleted = 0
    if going_offline:
        n_heartbeats_deleted = db.query(ImageWorkerHeartbeat).filter(
            ImageWorkerHeartbeat.worker_id == worker_id
        ).delete()

    if n or n_heartbeats_deleted:
        db.commit()
        if n:
            log.info(f"[image_platform] Worker '{worker_id}' released {n} own claim(s)")
        if n_heartbeats_deleted:
            log.info(f"[image_platform] Worker '{worker_id}' marked offline (heartbeat deleted)")

    return {"released": n, "heartbeat_deleted": bool(n_heartbeats_deleted)}


@router.post("/worker/jobs/{node_id}/release")
def worker_release_single_claim(
    node_id: int,
    worker_id: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db_session),
):
    """Release a single 'generating' claim back to 'queued'.

    Used by the worker when it's just claimed a job from a DIFFERENT
    batch while another batch is still in-flight on the current Flow
    project page. Switching projects mid-generation would strand the
    in-flight tiles, so the worker aborts the new claim and waits for
    the existing batch to drain first.

    Only releases if:
      - Node is currently 'generating'
      - claimed_by_worker matches the given worker_id (prevents one
        worker from releasing another worker's claim)

    Silent no-op if neither condition holds — idempotent.
    """
    _verify_worker_key(authorization)
    if not worker_id:
        raise HTTPException(400, "worker_id required")

    node = db.query(ImageNode).filter(ImageNode.id == node_id).first()
    if not node:
        raise HTTPException(404, f"Node {node_id} not found")

    released = False
    if node.status == "generating" and node.claimed_by_worker == worker_id:
        node.status = "queued"
        node.claimed_by_worker = None
        node.claimed_at = None
        node.error_message = None
        db.commit()
        released = True
        log.info(f"[image_platform] Worker '{worker_id}' released single claim on node {node_id}")

    return {"released": released, "node_id": node_id, "status": node.status}


@router.get("/worker/jobs/pending")
def worker_get_pending_job(
    request: Request,
    worker_id: Optional[str] = None,
    prefer_batch: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db_session),
):
    """Return the next queued image job for a worker to process, or
    {"job": null} if none.

    Atomic claim: sets status=generating, stamps claimed_by_worker +
    claimed_at. Jobs claimed >10 minutes ago without completion are
    released automatically.

    prefer_batch (v456): when the worker is currently processing a batch
    on its Flow page, passing the batch name prefix here lets the server
    prioritize same-batch nodes. Prevents the claim/release thrash that
    happens when a cross-batch job is handed out mid-processing — the
    worker can't switch Flow projects without stranding in-flight tiles,
    so it'd immediately release the cross-batch claim. Same-batch first
    keeps the pipeline moving; falls back to any queued node if the
    preferred batch has nothing queued (meaning the batch is fully
    submitted / downloading and we can safely switch).

    Response includes `input_image_urls`: list of URLs the worker can
    GET (with Bearer auth) to download the parent-variant images as
    reference inputs.
    """
    _verify_worker_key(authorization)
    _touch_worker_heartbeat(db, worker_id)

    # Release stale claims first (>10 min claimed + still generating)
    stale_cutoff = datetime.utcnow() - timedelta(minutes=10)
    stale_jobs = db.query(ImageNode).filter(
        ImageNode.status == "generating",
        ImageNode.claimed_at.isnot(None),
        ImageNode.claimed_at < stale_cutoff,
    ).all()
    for sj in stale_jobs:
        sj.status = "queued"
        sj.claimed_by_worker = None
        sj.claimed_at = None
        log.info(f"[image_platform] Released stale claim on node {sj.id}")
    if stale_jobs:
        db.commit()

    # Prefer same-batch nodes when the worker tells us which batch it's on.
    # The prefix is the name_prefix + "Scene" part — e.g. "Master Chen Batch "
    # matches names like "Master Chen Batch Scene 0", "Master Chen Batch Scene 1".
    node = None
    if prefer_batch:
        # Sanitize: strip trailing whitespace, cap length. The prefix is
        # user-controlled via node names so defense-in-depth on query construction.
        pb = prefer_batch.strip()[:200]
        if pb:
            # Match nodes whose name starts with the prefix. SQLAlchemy's
            # startswith() emits a LIKE with autoescaped wildcards.
            node = db.query(ImageNode).filter(
                ImageNode.status == "queued",
                ImageNode.name.startswith(pb),
            ).order_by(ImageNode.created_at.asc()).first()

    # Fall back to any queued node if no same-batch match (or no preference)
    if node is None:
        node = db.query(ImageNode).filter(
            ImageNode.status == "queued"
        ).order_by(ImageNode.created_at.asc()).first()

    if node is None:
        return {"job": None}

    # Resolve parent images to serve as URLs
    try:
        parent_paths = _resolve_parent_image_paths(db, node)
    except HTTPException as e:
        # A parent is no longer ready — fail this node so it doesn't
        # wedge the queue.
        node.status = "failed"
        node.error_message = f"Parent resolution failed: {e.detail}"
        db.commit()
        return {"job": None}

    # Build download URLs (authenticated via Bearer). For each parent edge,
    # we also emit a *stable filename* — the chosen variant's primary key —
    # so the worker can use it as gallery cache key. Once uploaded to Flow,
    # the image is stored in the gallery with alt=<filename>, so subsequent
    # scenes can look it up by name and skip re-upload.
    import hashlib
    base_url = str(request.base_url).rstrip("/")
    input_images: List[Dict[str, str]] = []

    # Re-walk the parent edges (sorted by slot) so we have both the file path
    # and the variant metadata together
    for edge in sorted(node.parent_edges, key=lambda e: e.slot_order or 0):
        parent = edge.parent
        if parent is None or parent.chosen_variant_id is None:
            continue
        chosen = next((v for v in parent.variants if v.id == parent.chosen_variant_id), None)
        if chosen is None:
            continue
        abs_path = str((images_root() / chosen.image_path).resolve())
        # Use the variant id as the stable filename. The .png extension is
        # what Flow expects. Worker saves this file locally under this name
        # and Flow's gallery alt-text will match.
        ext = Path(chosen.image_path).suffix.lower() or ".png"
        stable_name = f"variant_{chosen.id}{ext}"
        tok = hashlib.sha256(
            f"{abs_path}:{_get_worker_api_key()}".encode("utf-8")
        ).hexdigest()[:32]
        _worker_file_tokens[tok] = abs_path
        input_images.append({
            "url": f"{base_url}/api/images/worker/files/{tok}",
            "filename": stable_name,
            "role": edge.role or "",
            "slot_order": edge.slot_order or 0,
        })

    # Claim the job
    node.status = "generating"
    node.claimed_by_worker = worker_id or "unknown"
    node.claimed_at = datetime.utcnow()
    db.commit()

    # Keep backwards-compat: still emit input_image_urls (flat list) for
    # older worker versions, and the new input_images structure for v364+
    return {
        "job": {
            "id": node.id,
            "name": node.name or "",
            # v573: prepend per-slot reference manifest (see
            # _build_flow_prompt_with_manifest for rationale).
            "prompt": _resolve_flow_prompt_bindings(node),
            "aspect_ratio": node.aspect_ratio,
            "resolution": node.resolution,
            "model": node.model,
            "variants": int(node.n_variants or 1),
            "input_image_urls": [im["url"] for im in input_images],
            "input_images": input_images,
        }
    }


# In-memory file-token map. Tokens are valid until webapp restart —
# the worker is expected to download inputs immediately after claiming
# the job, so this is fine.
_worker_file_tokens: Dict[str, str] = {}


@router.get("/worker/files/{token}")
def worker_download_file(
    token: str,
    authorization: Optional[str] = Header(None),
):
    """Download a parent-variant image by its deterministic token.

    Token is sha256(abs_path + worker_api_key)[:32] — same as what
    _download_parent_reference constructs when issuing the job. To resolve
    it back to a path we scan the variants directory (cheap: a handful of
    images per project) and hash each to find the match. This survives
    webapp restarts (no in-memory state required) and R2 restores.
    """
    _verify_worker_key(authorization)

    # Fast path: check in-memory cache first (filled when we issued the job)
    path = _worker_file_tokens.get(token)

    # Slow path: scan filesystem to rebuild the mapping lazily.
    # After a webapp restart the in-memory map is empty but the files are
    # still on disk — and the token formula is deterministic, so we can
    # regenerate it from each candidate path and find the match.
    if not path:
        try:
            secret = _get_worker_api_key()
            root = images_root()
            # Walk only the generated variants dir to keep this O(n_variants)
            # not O(n_all_files). Typical projects have <1000 variants.
            for candidate in root.rglob("variant_*"):
                if not candidate.is_file():
                    continue
                abs_path = str(candidate.resolve())
                tok = hashlib.sha256(
                    f"{abs_path}:{secret}".encode("utf-8")
                ).hexdigest()[:32]
                if tok == token:
                    path = abs_path
                    # Cache the result for subsequent requests
                    _worker_file_tokens[tok] = abs_path
                    break
        except Exception:
            pass

    if not path:
        raise HTTPException(404, "Unknown file token (may be for a deleted variant)")

    p = Path(path)
    if not p.exists():
        # Try to restore from R2 backup
        try:
            rel = p.relative_to(images_root())
            _storage_download_to_local(str(rel).replace("\\", "/"))
        except Exception:
            pass
        if not p.exists():
            raise HTTPException(404, "File not found (and not in R2 backup)")
    return FileResponse(p)


@router.post("/worker/jobs/{node_id}/variants")
def worker_upload_variants(
    node_id: int,
    files: List[UploadFile] = File(...),
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db_session),
):
    """Worker uploads the N generated variant files for a claimed job.

    v473b: rewritten to minimize DB-connection hold time. Previously
    this function held a connection for the entire sequence (disk write
    + R2 upload + DB insert) which could take 5-30 seconds per request.
    Under active batch processing that exhausted the connection pool
    (up to 60 concurrent uploads x 15s each → pool timeout errors).
    Now: DB work happens in 3 quick bursts, with all slow I/O (R2
    uploads) done WITHOUT a DB connection held.
    """
    _verify_worker_key(authorization)

    # ==== Phase 1: quick DB validation + cleanup ====
    # Read node, verify state, clean stale variants. Commit, then
    # explicitly close this session to release the connection back to
    # the pool before we do slow I/O.
    node = db.query(ImageNode).filter(ImageNode.id == node_id).first()
    if not node:
        raise HTTPException(404, "Node not found")
    if node.status != "generating":
        raise HTTPException(409, f"Node not generating (status={node.status})")

    # Clean any stale variants/files (defensive)
    _delete_variant_files(node)
    for v in list(node.variants):
        db.delete(v)
    db.commit()

    # Close the dep-injected session early — releases the underlying
    # connection back to the pool so OTHER requests can use it while we
    # do the slow R2 uploads below. The dep wrapper's finally clause
    # will call db.close() again on the same session; that's a no-op
    # after the first close.
    db.close()

    # ==== Phase 2: slow I/O — disk writes + R2 uploads (NO DB held) ====
    out_dir = node_dir(node_id)
    pending_variants = []  # list of (idx, filename, rel_str, target_path)
    for idx, uf in enumerate(files, start=1):
        ext = Path(uf.filename or "").suffix.lower() or ".png"
        if ext not in (".png", ".jpg", ".jpeg", ".webp"):
            ext = ".png"
        filename = f"variant_{idx}{ext}"
        target = out_dir / filename
        try:
            content = uf.file.read()
            target.write_bytes(content)
        except Exception as e:
            log.error(f"[image_platform] Can't save variant {idx}: {e}")
            continue
        rel = target.relative_to(images_root())
        rel_str = str(rel).replace("\\", "/")
        # Mirror to R2 so it survives redeploys on ephemeral filesystems.
        # This is the slow part — no DB connection is held during it.
        try:
            _storage_upload_file(target, rel_str)
        except Exception as e:
            log.error(f"[image_platform] R2 upload failed for variant {idx}: {e}")
            # Still record the variant row — the file is on local disk
            # and can be re-uploaded later via the retry path.
        pending_variants.append((idx, filename, rel_str, target))

    # ==== Phase 3: quick DB insert of variant rows (fresh connection) ====
    saved_count = 0
    with get_db() as db2:
        # Re-read the node — we don't hold it across the session boundary.
        node2 = db2.query(ImageNode).filter(ImageNode.id == node_id).first()
        if not node2:
            # Node vanished while we were uploading — bail.
            raise HTTPException(404, "Node disappeared during upload")
        for idx, filename, rel_str, target in pending_variants:
            v = ImageVariant(
                node_id=node2.id,
                variant_index=idx,
                image_path=rel_str,
            )
            db2.add(v)
            saved_count += 1
        db2.commit()

    log.info(f"[image_platform] Saved {saved_count} variants for node {node_id}")
    return {"ok": True, "saved_count": saved_count}


class WorkerJobStatusRequest(BaseModel):
    status: str   # "completed" | "failed"
    error: Optional[str] = None


@router.post("/worker/jobs/{node_id}/status")
def worker_update_job_status(
    node_id: int,
    req: WorkerJobStatusRequest,
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db_session),
):
    """Worker marks a job done (success or failure)."""
    _verify_worker_key(authorization)
    node = db.query(ImageNode).filter(ImageNode.id == node_id).first()
    if not node:
        raise HTTPException(404, "Node not found")

    if req.status == "completed":
        # Only allow completion if there are variants saved
        n_variants = db.query(ImageVariant).filter(
            ImageVariant.node_id == node_id
        ).count()
        if n_variants == 0:
            node.status = "failed"
            node.error_message = "Worker reported completion but no variants uploaded"
        else:
            node.status = "ready"
            node.error_message = None
        node.claimed_by_worker = None
        node.claimed_at = None
    elif req.status == "failed":
        node.status = "failed"
        node.error_message = req.error or "Worker reported failure"
        node.claimed_by_worker = None
        node.claimed_at = None
    else:
        raise HTTPException(400, f"Unknown status: {req.status}")

    node.updated_at = datetime.utcnow()
    db.commit()
    return {"ok": True, "node_status": node.status}
