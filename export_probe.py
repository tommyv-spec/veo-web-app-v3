"""Probe the duration of a job's FINAL EXPORT mp4 (cached on the Job row).

Two jobs can share every WORD (the ED script bank is reused verbatim across
builds), so text cannot tell them apart. They are different RENDERS though, so
their exports differ in LENGTH — which is a cheap, hard discriminator.

Lazy by design: computing this at export time would need a backfill migration
for every historical job. Computing on demand caches the answer on first use and
self-heals every job that already exists.
"""
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "ffprobe")

# Same tuple main.py uses to decide "this job WAS exported" (see the has_export
# check around main.py:7734). Keep the two in step.
_FINAL_PREFIXES = ("final_export_", "final_broll_", "export_")


def probe_duration(path):
    """Seconds, or None. Never raises."""
    try:
        out = subprocess.run(
            [FFPROBE_BIN, "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
            capture_output=True, text=True, timeout=120,
        )
        val = (out.stdout or "").strip()
        return float(val) if val and val not in ("N/A",) else None
    except Exception as e:
        print(f"[export-probe] ffprobe failed on {path}: {e}", flush=True)
        return None


def newest_export_key(storage, job_id):
    """R2 key of the job's NEWEST final export, or None.

    Shared by the duration probe and the v854 waveform probe — both need the same
    "which mp4 did we actually ship" answer, and two copies of this lookup would
    drift the day the export naming changes.
    """
    keys = storage.list_objects(prefix=f"jobs/{job_id}/outputs/")
    finals = [k for k in keys if Path(k).name.startswith(_FINAL_PREFIXES)]
    if not finals:
        return None
    # final_export_<timestamp>_<hash>.mp4 — timestamp-prefixed, so a lexical sort
    # puts the newest export last.
    return sorted(finals)[-1]


def ensure_export_duration(db, job):
    """Duration of the job's final export, computed at most once per job.

    export_probed_at is stamped even on failure, so a job with no reachable
    export is not re-downloaded on every future match.
    """
    if job.export_duration_s is not None:
        return job.export_duration_s
    if job.export_probed_at is not None:
        return None  # already tried, nothing there

    from backends.storage import is_storage_configured, get_storage
    job.export_probed_at = datetime.utcnow()
    if not is_storage_configured():
        db.commit()
        return None
    try:
        storage = get_storage()
        key = newest_export_key(storage, job.id)
        if not key:
            print(f"[export-probe] job={job.id[:8]} no final export in R2", flush=True)
            db.commit()
            return None
        with tempfile.TemporaryDirectory() as td:
            local = str(Path(td) / "export.mp4")
            storage.download_file(key, local)
            dur = probe_duration(local)
        job.export_duration_s = dur
        db.commit()
        print(f"[export-probe] job={job.id[:8]} key={key} dur={dur}", flush=True)
        return dur
    except Exception as e:
        print(f"[export-probe] job={job.id[:8]} failed: {e}", flush=True)
        db.commit()
        return None
