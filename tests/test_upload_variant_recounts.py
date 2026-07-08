"""v825 — a MANUALLY uploaded clip variant must be RECOGNIZED by the job's
completed_clips counter (DONE tri-counter + progress bar).

Root cause (owner report 2026-07-08): upload_clip_variant set clip.status =
COMPLETED but never recomputed job.completed_clips, so an operator-uploaded
clip stayed uncounted — DONE stuck below TOTAL, job never flipped to completed.
Every OTHER completion path (worker uploads, attach) already bumps the counter.

Source-grep-assert (house style — py_compile does not catch a missing
recompute; see test_status_records_variant.py). We scope the assertions to the
upload_clip_variant function body so a recompute elsewhere cannot pass this.
"""
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_CODE = os.path.dirname(_HERE)
_MAIN = os.path.join(_CODE, "main.py")


def _upload_variant_body():
    """Return the source of the upload_clip_variant function only (from its
    def line to the next top-level `async def`/`def`/`@app`)."""
    src = open(_MAIN, encoding="utf-8").read()
    start = src.index("async def upload_clip_variant(")
    rest = src[start + 1:]
    # next top-level decorator or function definition ends the body
    ends = [
        rest.find("\n@app."),
        rest.find("\nasync def "),
        rest.find("\ndef "),
    ]
    ends = [e for e in ends if e != -1]
    end = min(ends) if ends else len(rest)
    return rest[:end]


def test_upload_variant_recomputes_completed_clips():
    body = _upload_variant_body()
    assert "job.completed_clips = completed" in body, (
        "upload_clip_variant must recompute job.completed_clips so a manual "
        "upload is counted in DONE"
    )
    assert "Clip.status == ClipStatus.COMPLETED.value" in body


def test_upload_variant_updates_progress_and_status():
    body = _upload_variant_body()
    assert "job.progress_percent" in body
    assert 'job.status = "completed"' in body


def test_upload_variant_guards_zero_total():
    """Must not false-flip a job to completed when total_clips is 0/None."""
    body = _upload_variant_body()
    assert "total = job.total_clips or 0" in body
    assert "if total > 0:" in body
