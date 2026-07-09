"""JobResponse pydantic schema — extracted so tests can import it without
loading the full FastAPI application (which has startup side effects)."""
from typing import Optional
from pydantic import BaseModel


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
    # === Post-render lifecycle tracker (2026-05-29) ===
    # `lifecycle_stage` is the EFFECTIVE stage (derived from has_export +
    # approval counts, see _build_job_response). Manual overrides
    # (awaiting_finishing past export, published) stick.
    lifecycle_stage: Optional[str] = None
    approval_at: Optional[str] = None
    export_at: Optional[str] = None
    finishing_at: Optional[str] = None
    published_at: Optional[str] = None
    notes: Optional[str] = None
    archived: bool = False
    stuck_days: Optional[int] = None
    approved_clips: int = 0
    instagram_url: Optional[str] = None
    published_via: Optional[str] = None
    # v780 — set on single-job GET only: the ImageJobBatch this video job was
    # promoted from (ImageJobBatch.promoted_video_job_id == job.id), so the UI
    # can offer a "go to image job" button. None for jobs not from an image job.
    source_image_batch_id: Optional[str] = None
    # v780.1 — the source image batch's human name (ImageJobBatch.name), so the
    # video job's Review & Approve header can show the build title on top instead
    # of only the UUID. Single-job GET only; None for jobs not from an image job.
    source_image_batch_name: Optional[str] = None
