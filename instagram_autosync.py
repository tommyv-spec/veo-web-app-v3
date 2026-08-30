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

# ---------------------------------------------------------------------------
# WHY THIS IS NOT A TIMER (2026-08-30)
#
# The first version synced the stalest account every 6 hours. That is a blind
# poll, and it is wrong in both directions at once: it spends HikerAPI pages
# when nothing has happened, and it is still up to SIX HOURS late when something
# has. A sync is not one call — fetch_recent_clips(limit=0) walks the account's
# whole history over several passes of up to 50 pages — so the waste is real.
#
# The system already KNOWS when a sync is worth doing. Three signals, strongest
# first, and a clock only as a backstop:
#
#   requested     Someone told us a post just went out for this account
#                 (publish_reel and the reconciler both nudge
#                 POST /api/instagram/accounts/{id}/request-sync). The worker
#                 ticks every second, so this lands in about a second instead
#                 of up to six hours.
#   never-synced  No picture at all yet.
#   waiting-jobs  A RECENT exported job has no published_at, so a reel for it
#                 may exist. Measured 2026-08-30 on the live platform: 252 jobs
#                 are exported-and-unpublished in total, but only 2 within 14
#                 days and ZERO within 3 — so scoped to the freshness window
#                 this gate is CLOSED almost always, while the unscoped version
#                 would have been permanently open and told us nothing.
#   backstop      Nothing said anything for a long time. This is the honest role
#                 of a clock: catching what no signal reports — a reel posted by
#                 hand from a phone, a repost, a deleted reel — not being the
#                 mechanism. Hence 24h, not 6.
#
# Net effect: faster when it matters, and it stops paying when it does not.
# ---------------------------------------------------------------------------

# The backstop, NOT the mechanism. Kept on the old env var so an operator who
# tuned it still controls the same thing.
BACKSTOP_HOURS = float(os.environ.get("IG_AUTOSYNC_INTERVAL_HOURS", "24"))
# Floor under the `waiting-jobs` signal. An exported job that is never going to
# be posted would otherwise hold the gate open on every one-second tick.
DISCOVERY_MINUTES = float(os.environ.get("IG_AUTOSYNC_DISCOVERY_MINUTES", "30"))
# Only jobs this fresh count as "a reel may exist for this". Matches the §R0.1
# 3-day freshness cap the posting lane already works to.
FRESH_DAYS = float(os.environ.get("IG_AUTOSYNC_FRESH_DAYS", "3"))
# How long a FAILED account waits before it is tried again.
DEFAULT_RETRY_MINUTES = float(os.environ.get("IG_AUTOSYNC_RETRY_MINUTES", "30"))
ENABLED = os.environ.get("IG_AUTOSYNC", "1").strip().lower() not in ("0", "false", "no")

# Back-compat: pick_account still accepts interval_hours, and callers that pass
# nothing get the backstop.
DEFAULT_INTERVAL_HOURS = BACKSTOP_HOURS

_ERROR_MAX = 500

# The candidate query scans the jobs table, and the worker ticks every SECOND.
# Memoised briefly so the cheap gate stays cheap — the answer cannot change
# meaningfully inside a minute, and a stale "yes" only costs one early sync.
_CAND_CACHE = {"at": None, "by_user": {}}
_CAND_TTL_S = 60.0


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


def request_sync(db, acc, reason="requested"):
    """Mark an account as owing a sync. The cheap half of the whole design.

    Costs one column write and no API call, so the caller that KNOWS something
    happened — publish_reel after a publish, the reconciler when it finds a live
    reel the platform has never seen — can say so directly instead of leaving
    the platform to discover it on a clock.
    """
    acc.sync_requested_at = datetime.utcnow()
    acc.sync_reason = (reason or "requested")[:64]
    return acc


def has_fresh_unpublished_export(db, user_id, now=None, fresh_days=None):
    """Could a reel plausibly exist that we have not matched yet?

    A reel on Instagram corresponds to a job that was EXPORTED. If that job also
    has no published_at, a sync might discover its reel. Scoped to recent jobs
    on purpose: unscoped this is true 252 times over on the live platform (old
    abandoned exports) and never closes, which would make it no gate at all.
    """
    from models import Job
    now = now or datetime.utcnow()
    fresh_days = FRESH_DAYS if fresh_days is None else fresh_days
    cutoff = now - timedelta(days=fresh_days)

    cached = _CAND_CACHE["by_user"].get(user_id)
    if (cached is not None and _CAND_CACHE["at"]
            and (now - _CAND_CACHE["at"]).total_seconds() < _CAND_TTL_S):
        return cached

    hit = (db.query(Job.id)
           .filter(Job.user_id == user_id,
                   Job.has_export == True,            # noqa: E712
                   Job.published_at.is_(None),
                   Job.archived == False,             # noqa: E712
                   Job.created_at >= cutoff)
           .first()) is not None
    if _CAND_CACHE["at"] is None or (now - _CAND_CACHE["at"]).total_seconds() >= _CAND_TTL_S:
        _CAND_CACHE["at"] = now
        _CAND_CACHE["by_user"] = {}
    _CAND_CACHE["by_user"][user_id] = hit
    return hit


def sync_reason_for(db, acc, now=None, retry_minutes=None, backstop_hours=None):
    """(priority, reason) for syncing this account now, or None to leave it.

    Lower priority number wins. Returning None is the common and correct answer:
    most of the time there is genuinely nothing to find, and a sync then is pure
    spend.
    """
    now = now or datetime.utcnow()
    retry_minutes = DEFAULT_RETRY_MINUTES if retry_minutes is None else retry_minutes
    backstop_hours = BACKSTOP_HOURS if backstop_hours is None else backstop_hours

    # A failing account backs off from EVERY reason, including a nudge — a
    # broken handle must not spin on a request it cannot satisfy.
    if (acc.last_sync_attempt_at
            and acc.last_sync_attempt_at > now - timedelta(minutes=retry_minutes)):
        return None

    requested = getattr(acc, "sync_requested_at", None)
    if requested and (not acc.last_synced_at or requested > acc.last_synced_at):
        # Self-clearing: once last_synced_at passes the request, this stops
        # firing. No second write, and no way to lose the flag.
        return (0, getattr(acc, "sync_reason", None) or "requested")

    if acc.last_synced_at is None:
        return (1, "never-synced")

    if acc.last_synced_at < now - timedelta(minutes=DISCOVERY_MINUTES):
        if has_fresh_unpublished_export(db, acc.user_id, now=now):
            return (2, "waiting-jobs")

    if acc.last_synced_at < now - timedelta(hours=backstop_hours):
        return (3, "backstop")

    return None


def pick_account(db, interval_hours=None, retry_minutes=None, now=None,
                 user_id=None):
    """(account, reason) most worth syncing right now, or (None, "").

    Strongest reason first; stalest account breaks a tie. `interval_hours` is
    kept as the backstop knob so existing callers and tests still mean what they
    meant.
    """
    from models import InstagramAccount
    now = now or datetime.utcnow()
    q = db.query(InstagramAccount)
    if user_id:
        q = q.filter(InstagramAccount.user_id == user_id)

    best = None
    for acc in q.all():
        got = sync_reason_for(db, acc, now=now, retry_minutes=retry_minutes,
                              backstop_hours=interval_hours)
        if got is None:
            continue
        priority, reason = got
        # Ordering in Python, not SQL: NULL ordering differs between Postgres
        # and SQLite, and "never synced" must sort FIRST on both. The account
        # table is a handful of rows.
        key = (priority, acc.last_synced_at or datetime.min)
        if best is None or key < best[0]:
            best = (key, acc, reason)
    return (best[1], best[2]) if best else (None, "")


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
    """One worker tick: sync at most one account THAT HAS A REASON. Returns how
    many ran — usually 0, which is the point.

    Wrapped end to end — this is called from the worker's main loop, so an
    exception escaping here would stop job processing.
    """
    if not ENABLED:
        return 0
    try:
        acc, reason = pick_account(db, interval_hours=interval_hours,
                                   retry_minutes=retry_minutes)
        if acc is None:
            return 0
        print(f"[ig-autosync] account={acc.id} @{acc.handle} syncing "
              f"reason={reason}", flush=True)
        sync_account_once(acc, db, fetcher=fetcher)
        return 1
    except Exception as exc:
        print(f"[ig-autosync] pass error: {type(exc).__name__}: {str(exc)[:200]}",
              flush=True)
        return 0
