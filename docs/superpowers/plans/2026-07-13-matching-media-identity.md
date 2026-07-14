# Matching Accuracy Phase 2 — Media Identity (Duration + Audio Fingerprint)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop *guessing* which job a posted video came from and start *proving* it — the posted reel IS the exported mp4, so its audio is byte-for-byte the same performance. Confirm the text matcher's top candidates against duration and an audio fingerprint.

**Architecture:** Text similarity ranks; **media identity decides.** A pure `audio_fingerprint.py` (ffmpeg → 8 kHz mono PCM → RMS energy envelope → normalized vector) produces a small comparable signature. Fingerprints are computed **lazily at match time for the top-2 candidates only** and cached on the row, so no export-path change and no migration backfill is needed — every existing job self-heals on first use. Duration is a free pre-filter (HikerAPI already returns `video_duration`; a local file is already on disk).

**Tech Stack:** Python 3, ffmpeg/ffprobe (already on PATH — `video_processor.py:22` uses them), SQLAlchemy, pytest. **No new dependencies** (no chromaprint, no numpy requirement).

**Prerequisite:** Phase 1 (`2026-07-13-matching-spoken-text-and-gates.md`) is merged.

---

## Why this is the only way to be "100% sure"

Text matching cannot be certain, because the scripts are *genuinely* near-identical — the Korella/Nuri builds share the ED language bank verbatim and differ only in hook and recipe (`instagram_match.py:77-88`). Two twins can be the same words. They cannot be the same **audio waveform**.

The posted reel is a re-encode of the exported mp4: same speech, same timing, same length. So:
- **Duration** is a hard, nearly-free discriminator. Verified live: HikerAPI returns `video_duration: 13.303999900817871` on the clip payload.
- **Audio envelope** (loudness over time) survives re-encoding and is essentially a signature of *that specific performance*. Cross-correlating it against the export's envelope is an identity check, not a similarity guess.

**Design decision — lazy, not eager.** An export-time hook would need a schema backfill for every historical job plus a change to the 600-line export path. Computing on demand for the **top-2 candidates only** and caching the result costs 2 R2 downloads the first time a job is ever considered, zero thereafter, and works retroactively on every job that already exists.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `audio_fingerprint.py` | Pure signal work: PCM → envelope, envelope compare, ffmpeg/ffprobe extraction | **Create** |
| `instagram_match.py` | Pure rules: duration gate, identity decision | **Modify** |
| `models.py` | `audio_fp` + `duration_s` columns on Job / InstagramVideo / LocalVideo + migrations | **Modify** |
| `instagram_client.py` | Parse `video_duration` off the clip payload | **Modify** |
| `main.py` | Store reel duration on sync; fingerprint-confirm the top-2 in `suggest_matches` | **Modify** |
| `local_transcribe.py` | Fingerprint the local file; confirm before auto-publishing | **Modify** |
| `test_audio_fingerprint.py` | Envelope + similarity tests on synthetic PCM | **Create** |
| `test_instagram_match.py` | Duration-gate + identity-decision tests | **Modify** |

---

### Task 1: The fingerprint module (pure signal, no DB, no network)

**Files:**
- Create: `audio_fingerprint.py`
- Create: `test_audio_fingerprint.py`

- [ ] **Step 1: Write the failing tests**

Create `test_audio_fingerprint.py`:

```python
"""Tests for audio_fingerprint — pure signal work on synthetic PCM."""
import array
import importlib.util
import math
import pathlib


def _load():
    spec = importlib.util.spec_from_file_location(
        "audio_fingerprint", pathlib.Path(__file__).parent / "audio_fingerprint.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _pcm(samples):
    """int list -> s16le bytes, as ffmpeg emits."""
    return array.array("h", samples).tobytes()


def _tone(n, amp):
    return [int(amp * math.sin(i / 8.0)) for i in range(n)]


def test_envelope_is_normalized_to_unit_length():
    m = _load()
    env = m.envelope_from_pcm(_pcm(_tone(8000, 10000)), frame_samples=200)
    norm = math.sqrt(sum(x * x for x in env))
    assert abs(norm - 1.0) < 1e-6


def test_envelope_tracks_loudness_over_time():
    """Quiet first half, loud second half -> later frames carry more energy."""
    m = _load()
    pcm = _pcm(_tone(4000, 500) + _tone(4000, 20000))
    env = m.envelope_from_pcm(pcm, frame_samples=200)
    half = len(env) // 2
    assert sum(env[half:]) > 3 * sum(env[:half])


def test_identical_audio_scores_one():
    m = _load()
    env = m.envelope_from_pcm(_pcm(_tone(8000, 9000)), frame_samples=200)
    assert m.envelope_similarity(env, env) > 0.999


def test_re_encoded_audio_still_scores_high():
    """A re-encode changes sample values slightly but not the loudness shape."""
    m = _load()
    clean = _tone(8000, 9000)
    noisy = [min(32767, max(-32768, s + (37 if i % 3 else -29))) for i, s in enumerate(clean)]
    a = m.envelope_from_pcm(_pcm(clean), frame_samples=200)
    b = m.envelope_from_pcm(_pcm(noisy), frame_samples=200)
    assert m.envelope_similarity(a, b) > 0.95


def test_different_performances_score_low():
    m = _load()
    a = m.envelope_from_pcm(_pcm(_tone(4000, 20000) + _tone(4000, 500)), frame_samples=200)
    b = m.envelope_from_pcm(_pcm(_tone(4000, 500) + _tone(4000, 20000)), frame_samples=200)
    assert m.envelope_similarity(a, b) < 0.6


def test_similarity_tolerates_a_small_offset():
    """IG trims a few frames off the head; alignment must survive that."""
    m = _load()
    base = _tone(2000, 1000) + _tone(4000, 18000) + _tone(2000, 1000)
    shifted = base[800:]  # chop 100ms @8kHz
    a = m.envelope_from_pcm(_pcm(base), frame_samples=200)
    b = m.envelope_from_pcm(_pcm(shifted), frame_samples=200)
    assert m.envelope_similarity(a, b) > 0.85


def test_serialization_roundtrips():
    m = _load()
    env = m.envelope_from_pcm(_pcm(_tone(4000, 7000)), frame_samples=200)
    restored = m.decode_fingerprint(m.encode_fingerprint(env))
    assert len(restored) == len(env)
    assert m.envelope_similarity(env, restored) > 0.999


def test_empty_pcm_yields_empty_envelope():
    m = _load()
    assert m.envelope_from_pcm(b"", frame_samples=200) == []
    assert m.envelope_similarity([], [1.0]) == 0.0
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd code && python -m pytest test_audio_fingerprint.py -v`
Expected: FAIL — `ModuleNotFoundError` / file not found

- [ ] **Step 3: Implement the module**

Create `audio_fingerprint.py`:

```python
"""Audio fingerprint — is this posted video the SAME RENDER as our export?

Text similarity can never be certain: near-duplicate scripts are genuinely
near-identical (the ED language bank is shared verbatim across builds). Two
builds can be the same WORDS. They cannot be the same WAVEFORM.

A posted reel is a re-encode of the exported mp4 — same performance, same
timing. So we compare the LOUDNESS ENVELOPE (RMS energy per ~25ms frame),
which survives re-encoding, resampling and loudness normalisation, but is a
signature of that specific take.

Deliberately dependency-free: ffmpeg (already on PATH, see video_processor.py)
plus stdlib. No chromaprint binary, no numpy requirement.
"""
import array
import base64
import math
import os
import struct
import subprocess

FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "ffprobe")

SAMPLE_RATE = 8000        # speech survives 8k; keeps the vector small
FRAME_SAMPLES = 200       # 25 ms @ 8 kHz
MAX_LAG_FRAMES = 40       # +/- 1.0 s of alignment slack (IG head/tail trims)


def envelope_from_pcm(pcm_bytes, frame_samples=FRAME_SAMPLES):
    """s16le mono PCM -> unit-length RMS energy envelope."""
    if not pcm_bytes:
        return []
    samples = array.array("h")
    usable = len(pcm_bytes) - (len(pcm_bytes) % 2)
    samples.frombytes(pcm_bytes[:usable])
    env = []
    for start in range(0, len(samples) - frame_samples + 1, frame_samples):
        acc = 0
        for i in range(start, start + frame_samples):
            s = samples[i]
            acc += s * s
        env.append(math.sqrt(acc / frame_samples))
    norm = math.sqrt(sum(x * x for x in env))
    if norm <= 0:
        return [0.0] * len(env)
    return [x / norm for x in env]


def _dot_at_lag(a, b, lag):
    """Dot product of a and b with b shifted by `lag` frames."""
    total = 0.0
    for i in range(len(a)):
        j = i + lag
        if 0 <= j < len(b):
            total += a[i] * b[j]
    return total


def envelope_similarity(a, b, max_lag=MAX_LAG_FRAMES):
    """Best normalized cross-correlation in [0, 1] over a small lag window.

    The lag search absorbs the head/tail trim a platform re-encode applies —
    without it, a 100 ms shift would tank an otherwise perfect match.
    """
    if not a or not b:
        return 0.0
    # Re-normalise over the overlapping region so a length difference cannot
    # inflate or deflate the score.
    best = 0.0
    for lag in range(-max_lag, max_lag + 1):
        num = _dot_at_lag(a, b, lag)
        # energy of the overlapping slices only
        ea = eb = 0.0
        for i in range(len(a)):
            j = i + lag
            if 0 <= j < len(b):
                ea += a[i] * a[i]
                eb += b[j] * b[j]
        denom = math.sqrt(ea * eb)
        if denom > 0:
            best = max(best, num / denom)
    return max(0.0, min(1.0, best))


def encode_fingerprint(env):
    """Envelope -> compact base64 (float32) for a TEXT column."""
    if not env:
        return ""
    raw = struct.pack(f"<{len(env)}f", *env)
    return base64.b64encode(raw).decode("ascii")


def decode_fingerprint(blob):
    if not blob:
        return []
    raw = base64.b64decode(blob)
    count = len(raw) // 4
    return list(struct.unpack(f"<{count}f", raw[:count * 4]))


def probe_duration(path):
    """Seconds, or None. Never raises — a missing duration must not break a match."""
    try:
        out = subprocess.run(
            [FFPROBE_BIN, "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
            capture_output=True, text=True, timeout=60,
        )
        val = (out.stdout or "").strip()
        return float(val) if val and val != "N/A" else None
    except Exception as e:
        print(f"[audio-fp] ffprobe failed on {path}: {e}", flush=True)
        return None


def fingerprint_file(path):
    """Media file -> (fingerprint_blob, duration_s). ('' , None) on failure.

    Decodes to 8 kHz mono s16le on stdout — no temp file.
    """
    try:
        proc = subprocess.run(
            [FFMPEG_BIN, "-v", "error", "-i", str(path),
             "-vn", "-ac", "1", "-ar", str(SAMPLE_RATE), "-f", "s16le", "-"],
            capture_output=True, timeout=300,
        )
        if proc.returncode != 0:
            print(f"[audio-fp] ffmpeg failed on {path}: "
                  f"{(proc.stderr or b'')[:200]!r}", flush=True)
            return "", None
        env = envelope_from_pcm(proc.stdout)
        return encode_fingerprint(env), probe_duration(path)
    except Exception as e:
        print(f"[audio-fp] fingerprint failed on {path}: {e}", flush=True)
        return "", None
```

- [ ] **Step 4: Run to verify they pass**

Run: `cd code && python -m pytest test_audio_fingerprint.py -v`
Expected: PASS — 8 passed

- [ ] **Step 5: Commit**

```bash
cd code
git add audio_fingerprint.py test_audio_fingerprint.py
git commit -m "feat(match): audio fingerprint - loudness envelope, ffmpeg + stdlib only"
```

---

### Task 2: Duration gate + identity decision (pure rules)

**Files:**
- Modify: `instagram_match.py` (append at end)
- Test: `test_instagram_match.py`

- [ ] **Step 1: Write the failing tests**

Append to `test_instagram_match.py`:

```python
# ---- v853: duration gate + media-identity decision ------------------------

def test_duration_gate_accepts_a_near_exact_match():
    m = _load()
    assert m.duration_plausible(13.30, 13.42) is True


def test_duration_gate_rejects_a_clearly_different_length():
    m = _load()
    assert m.duration_plausible(13.30, 21.90) is False


def test_duration_gate_never_excludes_on_missing_data():
    m = _load()
    assert m.duration_plausible(None, 13.4) is True
    assert m.duration_plausible(13.4, None) is True


def test_identity_confirms_on_high_fingerprint_similarity():
    m = _load()
    d = m.identity_decision(fp_similarity=0.97, duration_ok=True)
    assert d["identical"] is True
    assert d["reason"] == "audio-fingerprint"


def test_identity_refuses_when_duration_disagrees():
    """A high envelope score with the WRONG length is not the same render."""
    m = _load()
    d = m.identity_decision(fp_similarity=0.97, duration_ok=False)
    assert d["identical"] is False


def test_identity_refuses_on_low_fingerprint_similarity():
    m = _load()
    d = m.identity_decision(fp_similarity=0.40, duration_ok=True)
    assert d["identical"] is False


def test_identity_is_unknown_when_no_fingerprint_available():
    """No fingerprint must mean 'cannot tell', NOT 'not a match'."""
    m = _load()
    d = m.identity_decision(fp_similarity=None, duration_ok=True)
    assert d["identical"] is False
    assert d["reason"] == "no-fingerprint"
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd code && python -m pytest test_instagram_match.py -v -k "duration or identity"`
Expected: FAIL — `AttributeError: module 'instagram_match' has no attribute 'duration_plausible'`

- [ ] **Step 3: Implement**

Append to `instagram_match.py`:

```python
# ============================================================================
# v853 — MEDIA IDENTITY.
#
# The posted reel IS the exported mp4, re-encoded. Text can only ever say
# "these say similar things"; the waveform says "this is the same take".
#
# duration_plausible() is the free pre-filter (HikerAPI hands us video_duration
# on the clip payload; a local file is already on disk). The tolerance is
# generous ON PURPOSE: a platform re-encode may trim a few frames, and wrongly
# EXCLUDING the correct job is far worse than keeping an extra candidate that
# the fingerprint will then reject.
# ============================================================================

DURATION_TOLERANCE_S = 2.5
FP_IDENTICAL_MIN = 0.90


def duration_plausible(a_seconds, b_seconds, tolerance_s=DURATION_TOLERANCE_S):
    """False only when two durations are too far apart to be the same render.

    Unknown duration never excludes.
    """
    if a_seconds is None or b_seconds is None:
        return True
    return abs(float(a_seconds) - float(b_seconds)) <= tolerance_s


def identity_decision(fp_similarity, duration_ok, threshold=FP_IDENTICAL_MIN):
    """Is this posted video the same render as the candidate job's export?

    `identical` is only ever True on POSITIVE evidence. A missing fingerprint
    means "cannot tell" — never "not a match" — so the text ranking still
    stands on its own when the media evidence is unavailable.
    """
    if fp_similarity is None:
        return {"identical": False, "reason": "no-fingerprint", "similarity": None}
    if not duration_ok:
        return {"identical": False, "reason": "duration-mismatch",
                "similarity": round(fp_similarity, 4)}
    if fp_similarity >= threshold:
        return {"identical": True, "reason": "audio-fingerprint",
                "similarity": round(fp_similarity, 4)}
    return {"identical": False, "reason": "audio-differs",
            "similarity": round(fp_similarity, 4)}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cd code && python -m pytest test_instagram_match.py -v`
Expected: PASS — all (Phase 1 tests included)

- [ ] **Step 5: Commit**

```bash
cd code
git add instagram_match.py test_instagram_match.py
git commit -m "feat(match): duration gate + media-identity decision"
```

---

### Task 3: Schema — cache duration + fingerprint on the three row types

**Files:**
- Modify: `models.py` — `Job` (~line 232), `InstagramVideo` (~line 658), `LocalVideo` (~line 799), plus BOTH migration lists (postgres ~line 1101, sqlite ~line 1237)

- [ ] **Step 1: Add the columns**

In `class Job`, after `export_at`:

```python
    # v853 — media identity. Cached lazily on first match attempt (NOT at export
    # time): that needs no backfill migration and no change to the 600-line
    # export path, and every historical job self-heals the first time it is
    # considered as a candidate.
    export_duration_s = Column(Float, nullable=True)
    export_audio_fp = Column(Text, nullable=True)
    export_fp_at = Column(DateTime, nullable=True)   # NULL = never attempted
```

In `class InstagramVideo`, after `posted_at`:

```python
    # v853 — from the HikerAPI clip payload (`video_duration`) + computed at
    # transcription time, when the mp4 is already downloaded.
    duration_s      = Column(Float, nullable=True)
    audio_fp        = Column(Text, nullable=True)
```

In `class LocalVideo`, after `size_bytes`:

```python
    # v853 — computed at upload, when the file is already on disk.
    duration_s           = Column(Float, nullable=True)
    audio_fp             = Column(Text, nullable=True)
```

Add to the **postgres** migration list (next to the `published_via` entry, ~line 1101):

```python
        ("jobs", "export_duration_s", "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS export_duration_s DOUBLE PRECISION"),
        ("jobs", "export_audio_fp",   "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS export_audio_fp TEXT"),
        ("jobs", "export_fp_at",      "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS export_fp_at TIMESTAMP"),
        ("instagram_videos", "duration_s", "ALTER TABLE instagram_videos ADD COLUMN IF NOT EXISTS duration_s DOUBLE PRECISION"),
        ("instagram_videos", "audio_fp",   "ALTER TABLE instagram_videos ADD COLUMN IF NOT EXISTS audio_fp TEXT"),
        ("local_videos", "duration_s", "ALTER TABLE local_videos ADD COLUMN IF NOT EXISTS duration_s DOUBLE PRECISION"),
        ("local_videos", "audio_fp",   "ALTER TABLE local_videos ADD COLUMN IF NOT EXISTS audio_fp TEXT"),
```

Add to the **sqlite** migration list (~line 1237):

```python
        ("jobs", "export_duration_s", "ALTER TABLE jobs ADD COLUMN export_duration_s REAL"),
        ("jobs", "export_audio_fp",   "ALTER TABLE jobs ADD COLUMN export_audio_fp TEXT"),
        ("jobs", "export_fp_at",      "ALTER TABLE jobs ADD COLUMN export_fp_at DATETIME"),
        ("instagram_videos", "duration_s", "ALTER TABLE instagram_videos ADD COLUMN duration_s REAL"),
        ("instagram_videos", "audio_fp",   "ALTER TABLE instagram_videos ADD COLUMN audio_fp TEXT"),
        ("local_videos", "duration_s", "ALTER TABLE local_videos ADD COLUMN duration_s REAL"),
        ("local_videos", "audio_fp",   "ALTER TABLE local_videos ADD COLUMN audio_fp TEXT"),
```

- [ ] **Step 2: Verify the model imports and the columns exist**

Run:
```bash
cd code && python -c "
from models import Job, InstagramVideo, LocalVideo
for m in (Job, InstagramVideo, LocalVideo):
    cols = [c.name for c in m.__table__.columns]
    print(m.__name__, [c for c in cols if 'fp' in c or 'duration' in c])
"
```
Expected:
```
Job ['export_duration_s', 'export_audio_fp', 'export_fp_at']
InstagramVideo ['duration_s', 'audio_fp']
LocalVideo ['duration_s', 'audio_fp']
```

- [ ] **Step 3: Commit**

```bash
cd code
git add models.py
git commit -m "feat(match): cache duration + audio fingerprint on job/ig/local rows"
```

---

### Task 4: Capture the reel's duration on sync (free — already in the payload)

**Files:**
- Modify: `instagram_client.py` — `_clip_to_dict` return
- Modify: `main.py` — `sync_instagram_account` (both the update and insert branches) and `_ig_apply_counts` are untouched

- [ ] **Step 1: Parse it**

In `instagram_client.py`, inside `_clip_to_dict`, add before the `return`:

```python
    # v853 — the reel's length. Verified live on the HikerAPI clip payload:
    # video_duration: 13.303999900817871
    try:
        duration_s = float(m.get("video_duration")) if m.get("video_duration") else None
    except (TypeError, ValueError):
        duration_s = None
```

and add to the returned dict:

```python
        "duration_s": duration_s,
```

- [ ] **Step 2: Store it on sync**

In `main.py` `sync_instagram_account`, in the `if existing:` branch (next to the `posted_at` backfill):

```python
            if existing.duration_s is None and c.get("duration_s"):
                existing.duration_s = c["duration_s"]
```

and in the `InstagramVideo(...)` constructor for new rows:

```python
            duration_s=c.get("duration_s"),
```

- [ ] **Step 3: Verify against the live API**

Run (substitute the operator's key — never commit it):
```bash
cd code && HIKER_KEY=<key> python -c "
import os, instagram_client as ic
k = os.environ['HIKER_KEY']
uid = ic.resolve_user_id('natgeo', k)
clips = ic.fetch_recent_clips(uid, k, limit=3, max_pages=1)
for c in clips:
    print(c['shortcode'], 'duration_s=', c['duration_s'])
"
```
Expected: each line prints a real float (e.g. `duration_s= 13.303999900817871`), not `None`

- [ ] **Step 4: Commit**

```bash
cd code
git add instagram_client.py main.py
git commit -m "feat(match): capture reel duration from the HikerAPI payload"
```

---

### Task 5: Fingerprint the reel at transcription time

**Files:**
- Modify: `instagram_transcribe.py` — where the reel mp4 is downloaded for whisper

- [ ] **Step 1: Locate the download**

Run: `cd code && grep -n "video_url\|download\|def transcribe\|tmp\|\.mp4" instagram_transcribe.py | head -20`

The reel mp4 is already fetched to a local path there for whisper. Identify that path variable (call it `media_path` below).

- [ ] **Step 2: Fingerprint the same file, before it is deleted**

Immediately after transcription succeeds and while `media_path` still exists, add:

```python
        # v853 — fingerprint the SAME download whisper just used. Free: the
        # bytes are already on disk. This is what later proves the reel is our
        # export rather than a build that merely says similar words.
        try:
            from audio_fingerprint import fingerprint_file
            fp, dur = fingerprint_file(media_path)
            if fp:
                video.audio_fp = fp
            if dur and not video.duration_s:
                video.duration_s = dur
            print(f"[ig-fp] video={video.id} fp_frames={len(fp) // 8 if fp else 0} dur={dur}", flush=True)
        except Exception as e:
            # Never fail a transcription because a fingerprint failed.
            print(f"[ig-fp] video={video.id} fingerprint skipped: {e}", flush=True)
```

Ensure this runs before any `os.remove(media_path)` / tempdir cleanup, and that the `video` row is committed afterwards by the existing commit.

- [ ] **Step 3: Verify import**

Run: `cd code && python -c "import instagram_transcribe, audio_fingerprint; print('imports OK')"`
Expected: `imports OK`

- [ ] **Step 4: Commit**

```bash
cd code
git add instagram_transcribe.py
git commit -m "feat(match): fingerprint the reel during transcription"
```

---

### Task 6: Fingerprint the local file at upload

**Files:**
- Modify: `local_transcribe.py` — the upload/transcribe entry point that receives the uploaded file path

- [ ] **Step 1: Locate the saved upload path**

Run: `cd code && grep -n "def \|tmp\|\.mp4\|file_hash\|transcribe" local_transcribe.py | head -25`

Find the function that has the uploaded file on disk before transcription.

- [ ] **Step 2: Fingerprint it there**

After the transcription of the local file succeeds, while the path still exists:

```python
        # v853 — the local final-cut IS the export. Fingerprint it so the
        # auto-matcher can CONFIRM its pick instead of trusting the words alone.
        try:
            from audio_fingerprint import fingerprint_file
            fp, dur = fingerprint_file(media_path)
            if fp:
                video.audio_fp = fp
            if dur and not video.duration_s:
                video.duration_s = dur
            print(f"[local-fp] hash={video.file_hash[:8]} dur={dur}", flush=True)
        except Exception as e:
            print(f"[local-fp] hash={video.file_hash[:8]} fingerprint skipped: {e}", flush=True)
```

- [ ] **Step 3: Verify import**

Run: `cd code && python -c "import local_transcribe, audio_fingerprint; print('imports OK')"`
Expected: `imports OK`

- [ ] **Step 4: Commit**

```bash
cd code
git add local_transcribe.py
git commit -m "feat(match): fingerprint the local final-cut at upload"
```

---

### Task 7: Lazily fingerprint a job's export (cached, top-2 only)

**Files:**
- Create: helper in `main.py` (near the IG section, after `_ig_apply_counts`)

- [ ] **Step 1: Add the lazy cache helper**

```python
def _ensure_job_fingerprint(db, job) -> tuple:
    """(fingerprint, duration_s) for a job's FINAL EXPORT, computed at most once.

    Lazy on purpose. Hooking the export path would need a migration backfill for
    every historical job; computing here — only for the top candidates a match
    actually considers — costs one R2 download the first time a job is ever
    weighed, and zero every time after. export_fp_at is stamped even on failure
    so a job with no reachable export is not re-downloaded on every match.
    """
    if job.export_audio_fp:
        return job.export_audio_fp, job.export_duration_s
    if job.export_fp_at is not None:
        return None, job.export_duration_s   # already tried, nothing there

    import tempfile
    from pathlib import Path
    from backends.storage import is_storage_configured, get_storage
    from audio_fingerprint import fingerprint_file

    job.export_fp_at = datetime.utcnow()
    if not is_storage_configured():
        db.commit()
        return None, None
    try:
        storage = get_storage()
        keys = storage.list_objects(prefix=f"jobs/{job.id}/outputs/")
        finals = [
            k for k in keys
            if Path(k).name.startswith(("final_export_", "final_broll_", "export_"))
        ]
        if not finals:
            print(f"[job-fp] job={job.id[:8]} no final export in R2", flush=True)
            db.commit()
            return None, None
        key = sorted(finals)[-1]  # newest export wins
        with tempfile.TemporaryDirectory() as td:
            local = str(Path(td) / "export.mp4")
            storage.download_file(key, local)
            fp, dur = fingerprint_file(local)
        job.export_audio_fp = fp or None
        job.export_duration_s = dur
        db.commit()
        print(f"[job-fp] job={job.id[:8]} key={key} dur={dur} fp={'yes' if fp else 'no'}", flush=True)
        return job.export_audio_fp, job.export_duration_s
    except Exception as e:
        print(f"[job-fp] job={job.id[:8]} failed: {e}", flush=True)
        db.commit()
        return None, None
```

- [ ] **Step 2: Verify import**

Run: `cd code && python -c "import main; print('import OK')"`
Expected: `import OK`

- [ ] **Step 3: Commit**

```bash
cd code
git add main.py
git commit -m "feat(match): lazily cache a job's export fingerprint"
```

---

### Task 8: Confirm the IG match with media identity

**Files:**
- Modify: `main.py` — `suggest_matches`, after the ranking and before the return

- [ ] **Step 1: Insert the confirmation pass**

After `ranked = ranked_all[:5]` and before building `top`:

```python
    # v853 — the words RANK; the waveform DECIDES. Confirm only the top 2: the
    # fingerprint costs an R2 download the first time a job is weighed, so we
    # spend it on the candidates that could actually win, not the whole pool.
    identity = None
    from audio_fingerprint import decode_fingerprint, envelope_similarity
    reel_fp = decode_fingerprint(v.audio_fp) if v.audio_fp else None
    if reel_fp:
        by_id = {j.id: j for j in candidates}
        for r in ranked[:2]:
            job = by_id.get(r["job_id"])
            if not job:
                continue
            job_fp_blob, job_dur = _ensure_job_fingerprint(db, job)
            if not job_fp_blob:
                continue
            sim = envelope_similarity(reel_fp, decode_fingerprint(job_fp_blob))
            dur_ok = _ig_match.duration_plausible(v.duration_s, job_dur)
            decision = _ig_match.identity_decision(sim, dur_ok)
            r["identity"] = decision
            print(f"[ig-identity] video={video_id} job={job.id[:8]} "
                  f"sim={sim:.3f} dur_ok={dur_ok} -> {decision['reason']}", flush=True)
            if decision["identical"]:
                identity = {"job_id": job.id, **decision}
                break
    if identity:
        # Proven. Float it to the top and say so — this is no longer a guess.
        ranked.sort(key=lambda r: r["job_id"] != identity["job_id"])
        verdict = {"verdict": "identical", "top": 1.0, "gap": 1.0}
```

and extend the response dict:

```python
    return {"verdict": verdict["verdict"], "top": verdict["top"],
            "gap": verdict["gap"], "identity": identity, "suggestions": top}
```

- [ ] **Step 2: Show it in the popover**

In `static/index.html`, add to the `BANNER` map from Phase 1 Task 6:

```javascript
              identical: ["#10b981", "✓✓ PROVEN — the audio matches this job's export exactly."],
```

- [ ] **Step 3: Verify**

Run: `cd code && python -c "import main; print('import OK')" && python -m pytest test_instagram_match.py test_audio_fingerprint.py -v`
Expected: `import OK`, all tests pass

- [ ] **Step 4: Commit**

```bash
cd code
git add main.py static/index.html
git commit -m "feat(match): confirm the IG match against the export's audio"
```

---

### Task 9: Confirm the local auto-publish with media identity

**Files:**
- Modify: `local_transcribe.py` — the auto-match block (~line 166-180, where `auto_pick` runs)

This is the highest-stakes path: it **auto-publishes without a human**. A wrong auto-match here silently publishes the wrong job.

- [ ] **Step 1: Require proof before auto-publishing an ambiguous pick**

Replace the `pick_id = _ig_match.auto_pick(ranked, _MATCH_HIGH, _MATCH_MARGIN)` line and the block that follows with:

```python
        pick_id = _ig_match.auto_pick(ranked, _MATCH_HIGH, _MATCH_MARGIN)

        # v853 — the fingerprint can PROMOTE an otherwise-ambiguous pick (the
        # words could not separate two twins, but the waveform can) and can VETO
        # a confident one (right words, wrong render). This path auto-publishes
        # with no human in the loop, so proof beats confidence.
        if video.audio_fp:
            from audio_fingerprint import decode_fingerprint, envelope_similarity
            reel_fp = decode_fingerprint(video.audio_fp)
            by_id = {j.id: j for j in candidates}
            for r in ranked[:2]:
                job = by_id.get(r["job_id"])
                if not job:
                    continue
                blob, jdur = _ensure_job_fp(db, job)
                if not blob:
                    continue
                sim = envelope_similarity(reel_fp, decode_fingerprint(blob))
                dur_ok = _ig_match.duration_plausible(video.duration_s, jdur)
                decision = _ig_match.identity_decision(sim, dur_ok)
                print(f"[local-identity] hash={video.file_hash[:8]} job={job.id[:8]} "
                      f"sim={sim:.3f} dur_ok={dur_ok} -> {decision['reason']}", flush=True)
                if decision["identical"]:
                    pick_id = job.id          # PROMOTE: proven, even if ambiguous on text
                    break
                if pick_id == job.id and decision["reason"] in ("audio-differs", "duration-mismatch"):
                    print(f"[local-identity] VETO auto-pick {job.id[:8]} — "
                          f"text said yes, audio said no", flush=True)
                    pick_id = None            # VETO: never auto-publish over a media mismatch
                    break
```

- [ ] **Step 2: Share the lazy fingerprint helper**

`_ensure_job_fingerprint` lives in `main.py` but is needed here too, and `local_transcribe` must not import `main` (circular). Move the helper into `audio_fingerprint.py` as `ensure_job_fingerprint(db, job)` — it only needs `backends.storage` + `datetime`, not FastAPI — and import it from both. Update `main.py` Task 7 accordingly:

```python
# main.py
from audio_fingerprint import ensure_job_fingerprint as _ensure_job_fingerprint
```
```python
# local_transcribe.py
from audio_fingerprint import ensure_job_fingerprint as _ensure_job_fp
```

- [ ] **Step 3: Verify no circular import**

Run: `cd code && python -c "import main, local_transcribe, audio_fingerprint, instagram_match; print('imports OK')"`
Expected: `imports OK`

- [ ] **Step 4: Commit**

```bash
cd code
git add local_transcribe.py audio_fingerprint.py main.py
git commit -m "feat(match): audio proof can promote or VETO a local auto-publish"
```

---

### Task 10: Full suite + deploy + production verification

- [ ] **Step 1: Full test run**

Run: `cd code && python -m pytest test_instagram_match.py test_audio_fingerprint.py tests/test_local_watch_never_miss.py -v`
Expected: PASS, no failures

- [ ] **Step 2: Real-media sanity check (NOT a unit test)**

Fingerprint a real exported mp4 twice — once as-is, once re-encoded the way a platform would — and confirm the score stays high:

```bash
cd code && python -c "
import subprocess, sys
from audio_fingerprint import fingerprint_file, decode_fingerprint, envelope_similarity
src = sys.argv[1]
subprocess.run(['ffmpeg','-y','-v','error','-i',src,'-c:v','libx264','-crf','30','-c:a','aac','-b:a','96k','/tmp/reenc.mp4'], check=True)
a,da = fingerprint_file(src)
b,db_ = fingerprint_file('/tmp/reenc.mp4')
print('durations:', da, db_)
print('similarity:', envelope_similarity(decode_fingerprint(a), decode_fingerprint(b)))
" <path-to-a-real-exported-final_export_*.mp4>
```
Expected: `similarity:` **> 0.95**. If it is not, the threshold `FP_IDENTICAL_MIN` is wrong for real-world re-encodes — tune it on real data BEFORE shipping, do not guess.

- [ ] **Step 3: Push**

```bash
cd code && git push origin main
cd .. && git add code && git commit -m "build: bump code (matching phase 2 - media identity)"
```

- [ ] **Step 4: Verify in production**

Sync + transcribe a reel you KNOW the source job of, then click Match?. Read the Render log:

```
[ig-fp]       video=<id> fp_frames=<n> dur=<s>
[job-fp]      job=<id> key=jobs/<id>/outputs/final_export_... dur=<s> fp=yes
[ig-identity] video=<id> job=<id> sim=0.9xx dur_ok=True -> audio-fingerprint
```

Confirm the popover shows **"✓✓ PROVEN"** and names the correct job. Report the actual log lines — do NOT claim this works without them.

---

## Self-Review

**Spec coverage:**
- Tier 4 duration check → Tasks 2, 4 (IG payload), 6 (local), 7 (job export) ✅
- Tier 4 audio fingerprint → Tasks 1, 5, 6, 7 ✅
- IG path confirmation → Task 8 ✅
- Local-folder watcher confirmation (promote + veto) → Task 9 ✅
- Drive watcher → **not covered.** It shares `_bulk_dialogue_map` so it inherits Phase 1, but it has no fingerprint hook. Its uploaded file is on disk at transcription, so it can reuse Task 6's pattern — call it out as a follow-up rather than pretend it is done.

**Risks, stated plainly:**
1. **`FP_IDENTICAL_MIN = 0.90` is an untested guess.** Task 10 Step 2 tunes it on a real export before shipping. Do not skip that step.
2. `envelope_similarity` is O(lag × frames) in pure Python. For a 30 s clip that is ~81 lags × 1200 frames ≈ 100k operations — fine for 2 candidates per match, and the reason this only runs on the top 2 rather than the whole pool.
3. The lazy fingerprint downloads the export from R2 on first use. `export_fp_at` is stamped even on failure so a job with no reachable export is never re-downloaded on every subsequent match.
