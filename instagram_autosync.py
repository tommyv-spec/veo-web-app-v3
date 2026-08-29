"""Keep the platform's picture of Instagram current without anyone asking.

WHY THIS EXISTS. The platform learns that a job was published only through an
InstagramVideo -> Job match (main.py match_video), and that match can only happen
for a reel the platform has actually synced. Until 2026-08-29 the ONLY thing that
ever called the sync was publish_reel's kanban step, and that step was skipped
whenever the permalink was empty -- which is every SCHEDULED post, because a post
that fires hours later has no permalink at submit time.

So the more the posting ran unattended, the more the platform fell behind. On
2026-08-29 account 1 had not synced since 2026-07-04, account 4 was three days
stale, and three reels published overnight by Blotato were invisible: their jobs
still read `published_at: null`, and every inventory sweep offered videos that
were already live.

Nothing else was missing. The worker's existing transcription pass already
downloads a synced reel, fingerprints it, and evidence-matches it to a job
(instagram_transcribe._maybe_auto_match), which is what stamps published_at. Only
the sync call had no caller. This module is that caller.

DESIGN NOTES:
  * ONE ACCOUNT PER TICK. A sync paginates HikerAPI and costs roughly $0.001 a
    page; syncing every account on every tick would be both expensive and slow,
    and a slow account would stall the worker's other passes behind it.
  * THE STALEST ACCOUNT WINS. NULL sorts as infinitely stale, so a brand-new
    account is picked immediately.
  * TWO CLOCKS. `last_synced_at` moves only on success. A sync that raises never
    reaches it, so on its own a broken account would be retried on every tick
    forever -- `last_sync_attempt_at` is what makes the backoff possible, and it
    gates ONLY the account that failed, never the queue behind it.
  * IT RUNS IN THE WORKER, NOT IN A WEB REQUEST. A full sync of a 157-reel
    account exceeded gunicorn's timeout and dropped the connection when it was
    driven over HTTP on 2026-08-29. The worker has no such deadline.
"""
import os
from datetime import datetime, timedelta

# Every 6 hours per account: four accounts is ~16 syncs a day and a reel is
# never more than 6 hours from being seen.
#
# BE HONEST ABOUT THE COST — a sync is not one API call. Each one runs
# fetch_recent_clips(limit=0), which walks the account's WHOLE history over
# several passes of up to 50 pages (the /refresh-stats docstring says the same:
# "up to 50 HikerAPI calls" where that endpoint wants 1-3). So the real figure
# is pages, not syncs, and roughly an order of magnitude above "a couple of
# cents". It is still small, and this repo's standing rule is that paid calls
# get named rather than waved at. Turning the routine tick into a shallow fetch
# with one deep pass a day would cut it further; that is an operator call, not
# a defect, and the page counts behind it are inferred rather than measured.
DEFAULT_INTERVAL_HOURS = float(os.environ.get("IG_AUTOSYNC_INTERVAL_HOURS", "6"))
# How long a FAILED account waits before it is tried again.
DEFAULT_RETRY_MINUTES = float(os.environ.get("IG_AUTOSYNC_RETRY_MINUTES", "30"))
ENABLED = os.environ.get("IG_AUTOSYNC", "1").strip().lower() not in ("0", "false", "no")

_ERROR_MAX = 500


def _rollback(db) -> None:
    """Best-effort. A rollback that itself fails must not mask the real error."""
    try:
        db.rollback()
    except Exception:
        pass


def _commit(db) -> bool:
    """True when the commit landed. Never raises — every caller here is either
    a worker loop or an endpoint that has its own error contract."""
    try:
        db.commit()
        return True
    except Exception as exc:
        print(f"[ig-autosync] commit failed: {type(exc).__name__}: "
              f"{str(exc)[:200]}", flush=True)
        _rollback(db)
        return False


def apply_counts(row, clip: dict) -> None:
    """Copy view/like/comment counts onto an InstagramVideo row.

    A 0 from the API never overwrites a stored non-zero. The v1 chunk endpoint
    carries play_count for the ~12 newest reels; the v2 endpoints that serve
    everything older don't always, so an older reel comes back as 0 views — and
    a straight assignment wipes the real number we already had. Counts on a live
    reel don't fall to zero, so a 0 means "not reported", not "no views".
    """
    for field in ("views", "likes", "comments"):
        incoming = clip.get(field) or 0
        if incoming > 0 or not getattr(row, field):
            setattr(row, field, incoming)


def pick_account(db, interval_hours=None, retry_minutes=None, now=None,
                 user_id=None):
    """The one account most worth syncing right now, or None.

    Stalest first, skipping anything synced inside `interval_hours` and anything
    ATTEMPTED inside `retry_minutes`. The retry gate applies per account, so one
    permanently broken handle cannot starve the others.
    """
    from models import InstagramAccount
    now = now or datetime.utcnow()
    interval_hours = DEFAULT_INTERVAL_HOURS if interval_hours is None else interval_hours
    retry_minutes = DEFAULT_RETRY_MINUTES if retry_minutes is None else retry_minutes
    sync_cutoff = now - timedelta(hours=interval_hours)
    retry_cutoff = now - timedelta(minutes=retry_minutes)

    q = db.query(InstagramAccount)
    if user_id:
        q = q.filter(InstagramAccount.user_id == user_id)
    # Ordering in Python, not SQL: NULL ordering differs between Postgres and
    # SQLite, and "never synced" must mean FIRST on both. The account table is
    # a handful of rows.
    best = None
    for acc in q.all():
        if acc.last_synced_at and acc.last_synced_at > sync_cutoff:
            continue
        if acc.last_sync_attempt_at and acc.last_sync_attempt_at > retry_cutoff:
            continue
        key = acc.last_synced_at or datetime.min
        if best is None or key < best[0]:
            best = (key, acc)
    return best[1] if best else None


def _fetch_and_store(acc, db):
    """Pull this account's reels from HikerAPI and write them into the DB.

    Lifted out of main.py's /sync endpoint so the manual button and the
    unattended pass run the SAME code — two copies of this would drift, and the
    drift would show up as "it works when I click it".
    """
    from models import InstagramVideo
    from encryption import decrypt as _enc_decrypt
    from instagram_client import (resolve_user_id, fetch_recent_clips,
                                  ig_thumb_key, cache_thumb_bytes)
    from backends.storage import is_storage_configured, get_storage

    api_key = _enc_decrypt(acc.api_key_encrypted)
    if not acc.ig_user_id:
        acc.ig_user_id = resolve_user_id(acc.handle, api_key)
    # limit=0 → fetch all reels via cursor pagination (max 50 pages).
    clips = fetch_recent_clips(acc.ig_user_id, api_key, limit=0)

    # AN EMPTY ANSWER FROM A NON-EMPTY ACCOUNT IS A FAILURE, NOT A SYNC.
    # fetch_recent_clips swallows HikerAPIError on every pass and returns [],
    # so a total API outage looked exactly like "this account has no new reels":
    # last_synced_at advanced, last_sync_error was CLEARED (erasing a genuine
    # earlier failure), the 30-minute retry never engaged, and the account went
    # quiet for six hours showing green. That is precisely the blindness this
    # module exists to end, asserted as success. An account we already hold
    # reels for cannot legitimately return zero.
    if not clips:
        from models import InstagramVideo
        stored = db.query(InstagramVideo).filter_by(account_id=acc.id).count()
        if stored:
            raise RuntimeError(
                f"HikerAPI returned no clips for @{acc.handle} while {stored} "
                f"reel(s) are already stored — treating as an outage, not a sync")

    storage = get_storage() if is_storage_configured() else None
    added = 0
    thumbs_cached = 0
    for c in clips:
        if not c.get("shortcode"):
            continue
        existing = (db.query(InstagramVideo)
                    .filter_by(account_id=acc.id, shortcode=c["shortcode"]).first())
        if existing:
            apply_counts(existing, c)
            # Backfill posted_at on rows stored before the timestamp parser
            # handled string taken_at — NULL posted_at sinks the reel in the grid.
            if not existing.posted_at and c.get("posted_at"):
                existing.posted_at = c["posted_at"]
            # v853 — backfill the reel runtime on rows synced before we read it.
            if existing.duration_s is None and c.get("duration_s"):
                existing.duration_s = c["duration_s"]
            # Refresh signed URLs (they expire) so retries can re-download.
            if c.get("video_url"):
                existing.video_url = c.get("video_url")
            if c.get("thumb_url"):
                existing.thumb_url = c.get("thumb_url")
            # Cache the thumb bytes if not already cached (stale rows self-heal:
            # the line above just refreshed the dead url to a live one).
            if not existing.thumb_r2_key and existing.thumb_url:
                k = cache_thumb_bytes(existing.thumb_url,
                                      ig_thumb_key(acc.id, existing.shortcode), storage)
                if k:
                    existing.thumb_r2_key = k
                    thumbs_cached += 1
            continue
        thumb_key = None
        if c.get("thumb_url"):
            thumb_key = cache_thumb_bytes(c.get("thumb_url"),
                                          ig_thumb_key(acc.id, c["shortcode"]), storage)
            if thumb_key:
                thumbs_cached += 1
        db.add(InstagramVideo(
            account_id=acc.id,
            shortcode=c["shortcode"],
            url=c.get("url") or f"https://www.instagram.com/reel/{c['shortcode']}/",
            thumb_url=c.get("thumb_url"),
            thumb_r2_key=thumb_key,
            video_url=c.get("video_url"),
            caption=c.get("caption"),
            views=c.get("views") or 0,
            likes=c.get("likes") or 0,
            comments=c.get("comments") or 0,
            posted_at=c.get("posted_at"),
            duration_s=c.get("duration_s"),
        ))
        added += 1
    total = db.query(InstagramVideo).filter_by(account_id=acc.id).count()
    return {"added": added, "total": total, "thumbs_cached": thumbs_cached}


def sync_account_once(acc, db, fetcher=None):
    """Sync ONE account and record the outcome on both clocks.

    Returns {"ok", "added", "total", "thumbs_cached", "error"}. Never raises for
    an API failure — the caller is a worker loop, and a sync problem must not
    take job processing down with it. `fetcher` exists so the tests can run
    without HikerAPI.
    """
    fetch = fetcher or _fetch_and_store
    acc.last_sync_attempt_at = datetime.utcnow()
    try:
        res = fetch(acc, db) or {}
    except Exception as exc:
        # ROLLBACK FIRST. If the failure came from the database, the session is
        # already in a failed state and a commit on it raises
        # PendingRollbackError — out of a function whose docstring promises it
        # never raises, leaving BOTH clocks unwritten so nothing records why the
        # account went stale. The rollback also discards the attempt stamp set
        # above, so it has to be re-applied afterwards.
        _rollback(db)
        acc.last_sync_attempt_at = datetime.utcnow()
        acc.last_sync_error = f"{type(exc).__name__}: {exc}"[:_ERROR_MAX]
        if not _commit(db):
            print(f"[ig-autosync] account={acc.id} @{acc.handle} FAILED and the "
                  f"failure could not even be recorded: "
                  f"{type(exc).__name__}: {str(exc)[:160]}", flush=True)
            return {"ok": False, "added": 0, "total": 0, "thumbs_cached": 0,
                    "error": f"{type(exc).__name__}: {exc}"[:_ERROR_MAX]}
        print(f"[ig-autosync] account={acc.id} @{acc.handle} FAILED "
              f"{acc.last_sync_error}", flush=True)
        return {"ok": False, "added": 0, "total": 0, "thumbs_cached": 0,
                "error": acc.last_sync_error}
    acc.last_synced_at = datetime.utcnow()
    acc.last_sync_error = None
    # The success commit is where EVERY insert from the walk actually lands —
    # the sessionmaker is autoflush=False, so nothing flushed during the loop.
    # A unique-constraint clash with the manual Sync button racing this one used
    # to escape from here and discard the whole paid walk with both clocks
    # unwritten. Record the failure instead.
    if not _commit(db):
        _rollback(db)
        acc.last_sync_attempt_at = datetime.utcnow()
        acc.last_sync_error = "commit failed (concurrent sync?)"
        _commit(db)
        print(f"[ig-autosync] account={acc.id} @{acc.handle} fetched but the "
              f"commit failed — nothing stored", flush=True)
        return {"ok": False, "added": 0, "total": 0, "thumbs_cached": 0,
                "error": acc.last_sync_error}
    out = {"ok": True, "added": res.get("added", 0), "total": res.get("total", 0),
           "thumbs_cached": res.get("thumbs_cached", 0), "error": None}
    print(f"[ig-autosync] account={acc.id} @{acc.handle} ok "
          f"added={out['added']} total={out['total']}", flush=True)
    return out


def process_instagram_autosync(db, interval_hours=None, retry_minutes=None,
                               fetcher=None):
    """One worker tick: sync at most one stale account. Returns how many ran.

    Wrapped end to end — this is called from the worker's main loop, so an
    exception escaping here would stop job processing.
    """
    if not ENABLED:
        return 0
    try:
        acc = pick_account(db, interval_hours=interval_hours,
                           retry_minutes=retry_minutes)
        if acc is None:
            return 0
        sync_account_once(acc, db, fetcher=fetcher)
        return 1
    except Exception as exc:
        print(f"[ig-autosync] pass error: {type(exc).__name__}: {str(exc)[:200]}",
              flush=True)
        return 0
