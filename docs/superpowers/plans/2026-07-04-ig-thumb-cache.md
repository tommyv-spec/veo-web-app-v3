# Instagram Thumbnail Byte-Caching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop Instagram thumbnails from going black by caching the thumbnail bytes to R2 at sync time and serving them from our own same-origin URL, so they never expire and load fast.

**Architecture:** Root cause — the sync path stores the raw Instagram CDN url (`scontent-*.cdninstagram.com/...`) straight into `instagram_videos.thumb_url` ([main.py:3818](../../../main.py)) and the frontend renders it directly ([index.html:4685](../../../static/index.html)). IG CDN urls are signed + expire in hours/days and hotlink-block cross-origin, so after ~a day every tile turns black. Fix: at sync, download the thumb jpg and upload it to R2 (via the existing `storage.upload_bytes`), store the R2 key in a new `thumb_r2_key` column, and add a `GET /api/instagram/videos/{id}/thumb` route that 302-redirects to a presigned R2 url — mirroring the existing clip-serving pattern at [main.py:7121](../../../main.py). `to_dict` returns the same-origin route url when a cached key exists. A single **Sync** click both refreshes the (dead) IG urls AND caches the bytes, so the 14 already-stale rows self-heal on the next sync.

**Tech Stack:** FastAPI, SQLAlchemy, `requests` (already used in `instagram_client.py`), R2 via `backends/storage.py` (`get_storage`, `is_storage_configured`, `upload_bytes`, `get_presigned_url`).

---

## File Structure

- `code/instagram_client.py` — add pure, testable helpers: `ig_thumb_key()`, `cache_thumb_bytes()`, `thumb_public_url()`. This module already owns IG data-layer logic + imports `requests`.
- `code/models.py` — add `thumb_r2_key` column to `InstagramVideo`, wire it into `to_dict`, add the idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migration.
- `code/main.py` — call `cache_thumb_bytes()` inside the sync loop; add the `GET /api/instagram/videos/{id}/thumb` serve route.
- `code/test_ig_thumb_cache.py` — new test file (mirrors `test_instagram_match.py` importlib-load style; no DB/network needed for the pure helpers).

No frontend change required: the frontend already reads `v.thumb_url` with an `onerror` fallback, and `to_dict` now hands it the cached same-origin route.

---

## Task 1: Pure caching helpers in `instagram_client.py`

**Files:**
- Modify: `code/instagram_client.py` (add helpers near the top-level functions, after the existing imports)
- Test: `code/test_ig_thumb_cache.py`

- [ ] **Step 1: Write the failing tests**

Create `code/test_ig_thumb_cache.py`:

```python
"""Tests for instagram_client thumb-caching helpers."""
import importlib.util
import pathlib


def _load():
    spec = importlib.util.spec_from_file_location(
        "instagram_client", pathlib.Path(__file__).parent / "instagram_client.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeStorage:
    def __init__(self):
        self.uploaded = []

    def upload_bytes(self, data, remote_key, content_type="application/octet-stream", metadata=None):
        self.uploaded.append((remote_key, data, content_type))
        return remote_key


def test_ig_thumb_key_shape():
    m = _load()
    assert m.ig_thumb_key(7, "AbCdEf") == "instagram/thumbs/7/AbCdEf.jpg"


def test_cache_thumb_bytes_uploads_and_returns_key():
    m = _load()
    storage = _FakeStorage()
    key = m.cache_thumb_bytes(
        "https://scontent.example/x.jpg",
        "instagram/thumbs/7/AbCdEf.jpg",
        storage,
        fetch=lambda url, timeout: (b"\xff\xd8\xff\x00jpegbytes", "image/jpeg"),
    )
    assert key == "instagram/thumbs/7/AbCdEf.jpg"
    assert len(storage.uploaded) == 1
    assert storage.uploaded[0][0] == "instagram/thumbs/7/AbCdEf.jpg"
    assert storage.uploaded[0][2] == "image/jpeg"


def test_cache_thumb_bytes_none_when_no_url():
    m = _load()
    storage = _FakeStorage()
    assert m.cache_thumb_bytes(None, "k", storage, fetch=lambda u, t: (b"x", "image/jpeg")) is None
    assert storage.uploaded == []


def test_cache_thumb_bytes_none_when_storage_none():
    m = _load()
    assert m.cache_thumb_bytes("https://x/y.jpg", "k", None, fetch=lambda u, t: (b"x", "image/jpeg")) is None


def test_cache_thumb_bytes_swallows_fetch_error():
    m = _load()
    storage = _FakeStorage()

    def _boom(url, timeout):
        raise RuntimeError("network down")

    assert m.cache_thumb_bytes("https://x/y.jpg", "k", storage, fetch=_boom) is None
    assert storage.uploaded == []


def test_cache_thumb_bytes_none_when_empty_bytes():
    m = _load()
    storage = _FakeStorage()
    assert m.cache_thumb_bytes("https://x/y.jpg", "k", storage, fetch=lambda u, t: (b"", "image/jpeg")) is None
    assert storage.uploaded == []


def test_thumb_public_url_prefers_cached_route():
    m = _load()
    assert m.thumb_public_url(42, "instagram/thumbs/7/x.jpg", "https://scontent/dead.jpg") == "/api/instagram/videos/42/thumb"


def test_thumb_public_url_falls_back_to_raw():
    m = _load()
    assert m.thumb_public_url(42, None, "https://scontent/live.jpg") == "https://scontent/live.jpg"


def test_thumb_public_url_none_when_nothing():
    m = _load()
    assert m.thumb_public_url(42, None, None) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd code && python -m pytest test_ig_thumb_cache.py -v`
Expected: FAIL — `AttributeError: module 'instagram_client' has no attribute 'ig_thumb_key'`

- [ ] **Step 3: Implement the helpers**

In `code/instagram_client.py`, after the existing top-level imports (the file already has `import requests` at the top), add:

```python
def ig_thumb_key(account_id, shortcode: str) -> str:
    """R2 object key for a cached Instagram thumbnail. Stable per (account, reel)."""
    return f"instagram/thumbs/{account_id}/{shortcode}.jpg"


def _default_thumb_fetch(url: str, timeout: int):
    """Download thumb bytes from an IG CDN url. Returns (bytes, content_type)."""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content, resp.headers.get("Content-Type", "image/jpeg")


def cache_thumb_bytes(thumb_url, remote_key: str, storage, *, fetch=None, timeout: int = 10):
    """Download an IG thumbnail and upload the bytes to object storage.

    Returns `remote_key` on success, or None on any failure (missing url,
    no storage, download error, empty body). NEVER raises — a failed thumb
    cache must never break the sync request.

    `fetch` is injectable for tests: a callable (url, timeout) -> (bytes, content_type).
    """
    if not thumb_url or storage is None:
        return None
    fetch = fetch or _default_thumb_fetch
    try:
        data, content_type = fetch(thumb_url, timeout)
        if not data:
            return None
        storage.upload_bytes(
            data,
            remote_key,
            content_type=content_type or "image/jpeg",
        )
        return remote_key
    except Exception as e:  # noqa: BLE001 — best-effort cache, log + move on
        print(f"[IG thumb cache] failed {remote_key}: {e}", flush=True)
        return None


def thumb_public_url(video_id, thumb_r2_key, raw_thumb_url):
    """URL the frontend should use for a video's thumbnail.

    Prefer the cached same-origin route (never expires); fall back to the raw
    IG url (may be dead, but harmless — the frontend has an onerror placeholder);
    None if neither exists.
    """
    if thumb_r2_key:
        return f"/api/instagram/videos/{video_id}/thumb"
    return raw_thumb_url or None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd code && python -m pytest test_ig_thumb_cache.py -v`
Expected: PASS (9 passed)

- [ ] **Step 5: Verify the module still imports**

Run: `cd code && python -c "import instagram_client"`
Expected: no output, exit 0 (py_compile is insufficient per `code/CLAUDE.md`; the import catches missing-name regressions).

- [ ] **Step 6: Commit**

```bash
git add code/instagram_client.py code/test_ig_thumb_cache.py
git commit -m "feat: IG thumb byte-cache helpers (ig_thumb_key/cache_thumb_bytes/thumb_public_url)"
```

---

## Task 2: `thumb_r2_key` column + `to_dict` + migration in `models.py`

**Files:**
- Modify: `code/models.py:584` (column), `code/models.py:605-620` (to_dict), `code/models.py:1072-1075` (migration)

- [ ] **Step 1: Add the column**

In `code/models.py`, in the `InstagramVideo` class, immediately after the `thumb_url` column (line 584):

```python
    thumb_url       = Column(Text, nullable=True)
    thumb_r2_key    = Column(Text, nullable=True)  # R2 key of the cached thumb jpg; None = not cached yet
    video_url       = Column(Text, nullable=True)
```

- [ ] **Step 2: Wire `to_dict` to serve the cached route**

In `code/models.py`, change the `thumb_url` line inside `InstagramVideo.to_dict` (currently line 610) from:

```python
            "thumb_url": self.thumb_url,
```

to:

```python
            "thumb_url": _ig_thumb_public_url(self.id, self.thumb_r2_key, self.thumb_url),
```

At the top of `models.py`, add a lazy import helper near the other module-level imports so `models.py` does not hard-depend on `instagram_client` at import time (avoids any circular-import risk):

```python
def _ig_thumb_public_url(video_id, thumb_r2_key, raw_thumb_url):
    from instagram_client import thumb_public_url
    return thumb_public_url(video_id, thumb_r2_key, raw_thumb_url)
```

- [ ] **Step 3: Add the idempotent migration**

In `code/models.py`, extend the `alter_migrations` list (line 1072) to add the new column:

```python
    alter_migrations = [
        "ALTER TABLE instagram_videos ALTER COLUMN thumb_url TYPE TEXT",
        "ALTER TABLE instagram_videos ADD COLUMN IF NOT EXISTS video_url TEXT",
        "ALTER TABLE instagram_videos ADD COLUMN IF NOT EXISTS thumb_r2_key TEXT",
    ]
```

- [ ] **Step 4: Verify import + to_dict wiring**

Run: `cd code && python -c "import models; print(hasattr(models.InstagramVideo, 'thumb_r2_key'))"`
Expected: prints `True`, exit 0.

- [ ] **Step 5: Commit**

```bash
git add code/models.py
git commit -m "feat: add instagram_videos.thumb_r2_key column + serve cached url from to_dict"
```

---

## Task 3: Cache thumbs during sync in `main.py`

**Files:**
- Modify: `code/main.py:3799-3827` (the `sync_instagram_account` loop)

- [ ] **Step 1: Resolve storage once before the loop**

In `code/main.py`, in `sync_instagram_account`, immediately after `added = 0` (line 3799) and before `for c in clips:`, add:

```python
    added = 0
    # Best-effort thumb byte-cache: resolve storage once; None if unconfigured.
    from backends.storage import is_storage_configured, get_storage
    from instagram_client import ig_thumb_key, cache_thumb_bytes
    _ig_storage = get_storage() if is_storage_configured() else None
    _thumbs_cached = 0  # temporary diagnostic — see code/CLAUDE.md deploy discipline
```

- [ ] **Step 2: Cache on the existing-video branch (refresh path)**

In the `if existing:` branch, after `existing.thumb_url = c.get("thumb_url")` (line 3812), cache the bytes when this row has no cached key yet. Replace the existing block:

```python
            # Refresh signed URLs (they expire) so retries can re-download.
            if c.get("video_url"):
                existing.video_url = c.get("video_url")
            if c.get("thumb_url"):
                existing.thumb_url = c.get("thumb_url")
            continue
```

with:

```python
            # Refresh signed URLs (they expire) so retries can re-download.
            if c.get("video_url"):
                existing.video_url = c.get("video_url")
            if c.get("thumb_url"):
                existing.thumb_url = c.get("thumb_url")
            # Cache the thumb bytes if not already cached (stale rows self-heal:
            # the line above just refreshed the dead url to a live one).
            if not existing.thumb_r2_key and existing.thumb_url:
                _k = cache_thumb_bytes(
                    existing.thumb_url, ig_thumb_key(acc.id, existing.shortcode), _ig_storage
                )
                if _k:
                    existing.thumb_r2_key = _k
                    _thumbs_cached += 1
            continue
```

- [ ] **Step 3: Cache on the new-video branch**

Replace the new-video creation block (lines 3814-3827):

```python
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
```

with:

```python
        _thumb_key = None
        if c.get("thumb_url"):
            _thumb_key = cache_thumb_bytes(
                c.get("thumb_url"), ig_thumb_key(acc.id, c["shortcode"]), _ig_storage
            )
            if _thumb_key:
                _thumbs_cached += 1
        v = InstagramVideo(
            account_id=acc.id,
            shortcode=c["shortcode"],
            url=c.get("url") or f"https://www.instagram.com/reel/{c['shortcode']}/",
            thumb_url=c.get("thumb_url"),
            thumb_r2_key=_thumb_key,
            video_url=c.get("video_url"),
            caption=c.get("caption"),
            views=c.get("views") or 0,
            likes=c.get("likes") or 0,
            comments=c.get("comments") or 0,
            posted_at=c.get("posted_at"),
        )
        db.add(v)
        added += 1
```

- [ ] **Step 4: Log the diagnostic + return it**

Replace the tail of the function (lines 3828-3831):

```python
    acc.last_synced_at = datetime.utcnow()
    db.commit()
    total = db.query(InstagramVideo).filter_by(account_id=acc.id).count()
    return {"added": added, "total": total}
```

with:

```python
    acc.last_synced_at = datetime.utcnow()
    db.commit()
    total = db.query(InstagramVideo).filter_by(account_id=acc.id).count()
    # Temporary diagnostic (code/CLAUDE.md): confirms the next operator sync
    # actually cached bytes. Remove in a follow-up once evidence lands.
    print(f"[IG sync] account={acc.id} added={added} thumbs_cached={_thumbs_cached} storage={'on' if _ig_storage else 'off'}", flush=True)
    return {"added": added, "total": total, "thumbs_cached": _thumbs_cached}
```

- [ ] **Step 5: Verify import**

Run: `cd code && python -c "import main"`
Expected: no import error (may print startup logs; exit 0). If it needs env vars to import, instead run `cd code && python -m py_compile main.py && python -c "import ast; ast.parse(open('main.py').read())"` and note import could not be fully exercised.

- [ ] **Step 6: Commit**

```bash
git add code/main.py
git commit -m "feat: cache IG thumb bytes to R2 during sync (new + stale-row self-heal) + diagnostic"
```

---

## Task 4: Serve route `GET /api/instagram/videos/{id}/thumb` in `main.py`

**Files:**
- Modify: `code/main.py` — add a route next to the other `/api/instagram/videos/...` routes (e.g. right after `sync_instagram_account` ends, ~line 3832)

- [ ] **Step 1: Add the serve route**

Insert this route (place it after the `sync_instagram_account` function, before `list_instagram_videos` at line 3834). It mirrors the presigned-redirect pattern proven at `download_output` ([main.py:7199-7250](../../../main.py)):

```python
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
    storage = get_storage()
    presigned = storage.get_presigned_url(v.thumb_r2_key, expires_in=86400)
    return RedirectResponse(url=presigned, status_code=302, headers={"Cache-Control": "no-store"})
```

Note: `RedirectResponse` is already imported at [main.py:134](../../../main.py). Confirm `_get_user_ig_account` uses `account.user_id == current_user.id` ownership — the join filter above matches that convention.

- [ ] **Step 2: Verify import**

Run: `cd code && python -c "import main"`
Expected: no import error (exit 0), or fall back to the ast-parse check from Task 3 Step 5 with a note.

- [ ] **Step 3: Verify the route is registered**

Run:
```bash
cd code && python -c "import main; print([r.path for r in main.app.routes if 'thumb' in getattr(r,'path','')])"
```
Expected: prints a list containing `/api/instagram/videos/{video_id}/thumb`.

- [ ] **Step 4: Commit**

```bash
git add code/main.py
git commit -m "feat: GET /api/instagram/videos/{id}/thumb — presigned R2 redirect for cached thumbs"
```

---

## Task 5: End-to-end verification (before claiming done)

Per root `CLAUDE.md` §2 — no "should work" without evidence. `code/` auto-deploys to Render on push to `main`; production is the only environment.

- [ ] **Step 1: Push + wait for deploy**

```bash
git push origin main
```
Wait ~3 min for Render to redeploy.

- [ ] **Step 2: Confirm the migration ran**

Ask the operator (or check Render logs) for the boot line:
`[Migration] PostgreSQL: ALTER ok — ALTER TABLE instagram_videos ADD COLUMN IF NOT EXISTS thumb_r2_key TEXT`

- [ ] **Step 3: Operator clicks Sync on `@healthy_men_with_nuri`**

This refreshes the 14 dead urls AND caches their bytes. Expected server log:
`[IG sync] account=<id> added=0 thumbs_cached=14 storage=on`
(If `storage=off` → R2 not configured on this deploy; STOP and surface — caching cannot work and thumbs will keep falling back to raw dead urls.)

- [ ] **Step 4: Confirm the API now returns same-origin thumb urls**

Operator captures the response of `GET /api/instagram/accounts/<id>/videos` (DevTools → Network, or the reloaded Instagram tab). Expected: each video's `thumb_url` is `"/api/instagram/videos/<id>/thumb"`, NOT a `scontent-*.cdninstagram.com` url.

- [ ] **Step 5: Confirm tiles render (the actual bug fixed)**

Operator reloads the Instagram tab. Expected: thumbnails visible, not black. Capture a screenshot as evidence. A `GET /api/instagram/videos/<id>/thumb` in the Network tab should be `302 → <r2-presigned> → 200 image/jpeg`.

- [ ] **Step 6: Spawn the post-push reviewer**

Per `code/CLAUDE.md`: after every push to `code/` main, spawn `caveman:cavecrew-reviewer` on the commit set.

- [ ] **Step 7: Remove the diagnostic (follow-up commit, only after Step 3 evidence lands)**

Once the `thumbs_cached=N` line has confirmed caching works, remove the `_thumbs_cached` counter + the `[IG sync]` print from `sync_instagram_account` (keep the `thumbs_cached` field in the return — it is cheap + useful). Commit:
```bash
git add code/main.py && git commit -m "chore: drop IG sync thumb-cache diagnostic (evidence landed)"
```

---

## Self-Review notes

- **Spec coverage:** black-thumb root cause (expired urls) → Tasks 1-4 replace serving with permanent same-origin R2 urls; the 14 stale rows → self-heal via sync refresh (Task 3 Step 2) + verified in Task 5 Step 3.
- **Loading optimization:** same-origin + R2 presigned bytes = no cross-origin hotlink block, R2 edge bandwidth in the byte path (origin only signs), browser caches per presigned response headers. Thumbs load like our own clips do.
- **No frontend change:** `to_dict` swaps `thumb_url` to the cached route, and `index.html:4685` already consumes `v.thumb_url` with an onerror placeholder — nothing to touch there.
- **Failure isolation:** `cache_thumb_bytes` never raises (Task 1) — a dead thumb or R2 hiccup logs + returns None; sync still completes and the row simply keeps its raw-url fallback.
- **Type consistency:** `ig_thumb_key(account_id, shortcode)`, `cache_thumb_bytes(thumb_url, remote_key, storage, *, fetch, timeout)`, `thumb_public_url(video_id, thumb_r2_key, raw_thumb_url)` — names + signatures identical across Tasks 1-4.
- **Not built (YAGNI):** thumbnail resize/webp re-encode (IG candidate[0] is already small; add later only if payload is a problem), and a separate backfill endpoint (folding into Sync covers the stale rows in one click).
