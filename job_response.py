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
    lifecycle_stage: Optional[str] = None
    approval_at: Optional[str] = None
    export_at: Optional[str] = None
    finishing_at: Optional[str] = None
    published_at: Optional[str] = None
    notes: Optional[str] = None
    archived: bool = False
    stuck_days: Optional[int] = None
