"""Probe the duration of a job's FINAL EXPORT mp4 (cached on the Job row).

Two jobs can share every WORD (the ED script bank is reused verbatim across
builds), so text cannot tell them apart. They are different RENDERS though, so
their exports differ in LENGTH — which is a cheap, hard discriminator.

Lazy by design: computing this at export time would need a backfill migration
for every historical job. Computing on demand caches the answer on first use and
self-heals every job that already exists.
"""
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path

FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "ffprobe")

# Same tuple main.py uses to decide "this job WAS exported" (see the has_export
# check around main.py:7734). Keep the two in step.
_FINAL_PREFIXES = ("final_export_", "final_broll_", "export_")

# The YYYYMMDD_HHMMSS an export is minted with. The `(?!\d)` matters: a v856
# name is final_export_<job8>_<ts>_<hash>, and an all-decimal job id (e.g.
# 12345678) would otherwise let "12345678_202607" match as the timestamp. A real
# timestamp is followed by "_", never by another digit.
_TS_RE = re.compile(r"(\d{8}_\d{6})(?!\d)")


def _export_timestamp(name):
    """The export's YYYYMMDD_HHMMSS, or "" when the name carries none.

    "" sorts first, i.e. an unparseable name is treated as the OLDEST — it can
    never displace a name we can actually date.
    """
    m = _TS_RE.search(name or "")
    return m.group(1) if m else ""


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
    # Sort on the TIMESTAMP, not on the whole name.
    #
    # It used to be `sorted(finals)[-1]`, which worked only because the name
    # began `final_export_<timestamp>_...` — the timestamp was the first thing
    # that varied, so lexical order WAS chronological order. v856 stamps the
    # job id in ahead of the timestamp and quietly breaks that assumption: for a
    # job with both a legacy export and a re-export, the legacy name starts
    # "2026..." while the new one starts with the job id, and any job id below
    # "2026..." (hex, so most of them) sorts the NEWER file FIRST — handing the
    # OLD mp4 back as "newest". The probe would then fingerprint the wrong
    # render and every downstream evidence match would be measured against it.
    #
    # Read the timestamp out explicitly instead. Name stays the tie-break, which
    # preserves the old within-timestamp order (final_broll_ < final_export_, so
    # the speaker export still wins over its b-roll twin).
    return sorted(finals, key=lambda k: (_export_timestamp(Path(k).name), k))[-1]


# How many jobs a single match is allowed to probe from R2. Each probe is a
# download + ffmpeg, and they CACHE on the Job row — so the cap only bounds the
# FIRST match that meets a brand-new job. Six covers the text-ranked shortlist;
# probing the whole pool inside a web request is what times the worker out.
LAZY_PROBE_CAP = 6

# ...and how LONG they may take in total. A COUNT cap is not a TIME cap: one
# stalled R2 leg can hold a single probe for minutes (botocore read timeout ×
# adaptive retries, then ffmpeg/ffprobe subprocess timeouts), so six probes can
# still blow past gunicorn's --timeout 300 and get the worker SIGABRT'd — the
# exact shape of the 2026-07-06 outage (see local_transcribe._bulk_dialogue_map).
# Stopping early is SAFE: an un-probed candidate carries NULL duration/fp, and
# evidence_pick reads a NULL as an abstention, so the match falls back to a
# manual pick instead of guessing.
LAZY_PROBE_BUDGET_S = float(os.environ.get("LAZY_PROBE_BUDGET_S", "20"))


def evidence_candidates(db, jobs, priority_ids=(), max_probe=LAZY_PROBE_CAP,
                        budget_s=LAZY_PROBE_BUDGET_S):
    """[{job_id, export_duration_s, export_audio_fp}] — the input to evidence_pick.

    A brand-new job has NULL media columns (nothing has looked at its export
    yet), so the top `max_probe` candidates named by `priority_ids` (the text
    ranking's shortlist) are probed lazily here. Everything else is reported
    with whatever is already cached — a NULL simply means "this candidate brings
    no evidence", which evidence_pick treats as an abstention, never a verdict.

    Two bounds, both needed: `max_probe` (how many) and `budget_s` (how long,
    wall-clock, checked BEFORE each probe). Pass budget_s=None to disable the
    clock — only ever appropriate off the request path (e.g. an offline
    backfill). A caller that already runs under its own wall-clock guard should
    pass its REMAINING budget down, or its guard is a lie.
    """
    by_id = {j.id: j for j in jobs}
    probed = 0
    skipped_budget = 0
    t0 = time.monotonic()
    for jid in priority_ids:
        if probed >= max_probe:
            break
        j = by_id.get(jid)
        if j is None:
            continue
        needs_fp = j.export_audio_fp is None
        needs_dur = j.export_duration_s is None and j.export_probed_at is None
        if not (needs_fp or needs_dur):
            continue  # already cached — costs nothing, not a probe
        if budget_s is not None and (time.monotonic() - t0) >= budget_s:
            skipped_budget += 1
            continue
        if needs_fp:
            # Fills export_duration_s too while the mp4 is on disk — one download.
            ensure_export_fingerprint(db, j)
        else:
            ensure_export_duration(db, j)
        probed += 1
    if skipped_budget:
        print(
            f"[export-probe] budget {budget_s}s exhausted after {probed} probe(s) "
            f"({time.monotonic() - t0:.1f}s) — {skipped_budget} candidate(s) left "
            f"un-probed; they abstain",
            flush=True,
        )
    return [
        {
            "job_id": j.id,
            "export_duration_s": j.export_duration_s,
            "export_audio_fp": j.export_audio_fp,
        }
        for j in jobs
    ]


def ensure_export_fingerprint(db, job):
    """Loudness-envelope fingerprint of the job's final export, computed at most
    once per job. Returns the base64 blob, or "" when there is nothing to print.

    v855 — THE shared helper. The diag backfill endpoint, the IG watcher, the
    local/drive watchers and the manual suggestions popover all need the same
    answer ("what does this job's export SOUND like"), and four copies of the
    R2-download-then-ffmpeg dance would drift the day export naming changes.

    Sentinel, same shape as export_probed_at: NULL means "never looked", ""
    means "looked, nothing usable" — so a job with no reachable export is never
    re-downloaded on every future match.
    """
    if job.export_audio_fp is not None:
        return job.export_audio_fp
    from audio_fingerprint import fingerprint_media
    from backends.storage import is_storage_configured, get_storage
    if not is_storage_configured():
        return ""   # not a fact about the job — do NOT write the "" sentinel
    blob = ""
    try:
        storage = get_storage()
        key = newest_export_key(storage, job.id)
        if key:
            # v858 — self-heal the basename<->job lookup key on any historical
            # job, reusing the key we already resolved (no extra R2 call).
            if job.export_basename is None:
                job.export_basename = Path(key).stem
            with tempfile.TemporaryDirectory() as td:
                local = str(Path(td) / "export.mp4")
                storage.download_file(key, local)
                blob = fingerprint_media(local)
                # The export is already on disk and ffprobe is cheap — fill the
                # duration too rather than paying for the same download twice.
                if job.export_duration_s is None:
                    dur = probe_duration(local)
                    if dur is not None:
                        job.export_duration_s = dur
                        job.export_probed_at = datetime.utcnow()
        else:
            print(f"[audio-fp] job={job.id[:8]} no final export in R2", flush=True)
    except Exception as e:
        print(f"[audio-fp] job={job.id[:8]} export fp failed: {e}", flush=True)
        blob = ""
    job.export_audio_fp = blob   # "" on failure -> not NULL -> never re-downloaded
    db.commit()
    return blob


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
        # v858 — self-heal the basename<->job lookup key, reusing the resolved key.
        if job.export_basename is None:
            job.export_basename = Path(key).stem
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
