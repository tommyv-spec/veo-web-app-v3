"""The worker syncs Instagram on its own now — these are the rules it follows.

Before 2026-08-29 nothing ever called /api/instagram/accounts/{id}/sync except
publish_reel's kanban step, and that step was skipped whenever the permalink was
empty — which is every SCHEDULED post, because a scheduled post has no permalink
at submit time. The result: account 1 had not synced since 2026-07-04, and three
reels published by Blotato while nobody was awake were invisible to the platform,
so their jobs kept reporting published_at: null and every inventory sweep offered
them as fresh stock.

The platform already knew what to do with a synced reel — the worker's existing
transcription pass evidence-matches it to a job and stamps published_at. Only the
sync call was missing. That is all this module adds.

Run: python -m pytest code/tests/test_instagram_autosync.py -q
"""
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import image_platform  # noqa: F401 — registers image_nodes, which clips FKs to
import instagram_autosync as autosync
from models import Base, User, InstagramAccount


@pytest.fixture
def db():
    eng = create_engine("sqlite://")
    Base.metadata.create_all(eng)
    s = sessionmaker(bind=eng)()
    s.add(User(id="u1", email="o@x.com"))
    s.commit()
    yield s
    s.close()


def account(s, handle, synced=None, attempted=None, error=None):
    a = InstagramAccount(user_id="u1", handle=handle, ig_user_id="1",
                         api_key_encrypted="k", last_synced_at=synced,
                         last_sync_attempt_at=attempted, last_sync_error=error)
    s.add(a)
    s.commit()
    return a


class TestPickAccount:
    """v953.4 — the picker is REASON-driven, not a clock.

    pick_account returns (account, reason). The old version synced the stalest
    account every 6 hours whether or not anything had happened: it paid for a
    full history walk on an idle account AND still left a real post invisible
    for up to six hours. `interval_hours` now means the BACKSTOP.
    """

    def test_the_stalest_account_wins_a_tie_on_the_backstop(self, db):
        now = datetime.utcnow()
        account(db, "recent", synced=now - timedelta(hours=7))
        old = account(db, "ancient", synced=now - timedelta(days=56))
        acc, reason = autosync.pick_account(db, interval_hours=6, now=now)
        assert acc.id == old.id
        assert reason == "backstop"

    def test_an_idle_account_with_nothing_to_find_is_LEFT_ALONE(self, db):
        # The whole point. Nothing happened, no fresh unpublished export, inside
        # the backstop -> do not spend a history walk to learn nothing.
        autosync._CAND_CACHE["at"] = None
        now = datetime.utcnow()
        account(db, "idle", synced=now - timedelta(hours=7))
        acc, reason = autosync.pick_account(db, interval_hours=24, now=now)
        assert acc is None, f"synced an idle account for no reason ({reason})"

    def test_a_nudge_beats_everything_and_fires_immediately(self, db):
        # publish_reel says "I just posted to this account". The worker ticks
        # every second, so this must be pickable AT ONCE, not on a clock.
        autosync._CAND_CACHE["at"] = None
        now = datetime.utcnow()
        account(db, "stale", synced=now - timedelta(days=9))
        fresh = account(db, "just-posted", synced=now - timedelta(minutes=1))
        autosync.request_sync(db, fresh, "published")
        db.commit()
        acc, reason = autosync.pick_account(db, interval_hours=24, now=now)
        assert acc.id == fresh.id, "a nudge must outrank a stale account"
        assert reason == "published"

    def test_a_nudge_stops_firing_once_the_sync_has_happened(self, db):
        # Self-clearing by design: last_synced_at passing the request retires
        # it, so there is no flag to lose and no second write.
        autosync._CAND_CACHE["at"] = None
        now = datetime.utcnow()
        a = account(db, "done", synced=now - timedelta(minutes=1))
        autosync.request_sync(db, a, "published")
        db.commit()
        assert autosync.pick_account(db, interval_hours=24, now=now)[0] is not None
        a.last_synced_at = datetime.utcnow()
        db.commit()
        acc, _ = autosync.pick_account(db, interval_hours=24, now=datetime.utcnow())
        assert acc is None, "the nudge fired twice"

    def test_a_never_synced_account_is_picked(self, db):
        # NULL last_synced_at must sort as infinitely stale, not as "no opinion".
        now = datetime.utcnow()
        a = account(db, "new")
        acc, reason = autosync.pick_account(db, interval_hours=6, now=now)
        assert acc.id == a.id
        assert reason == "never-synced"

    def test_a_fresh_unpublished_export_is_a_reason_to_look(self, db):
        # A reel can only exist for a job that was EXPORTED. If one is recent
        # and still unpublished, a sync might find its reel — a signal, not a
        # guess.
        from models import Job
        autosync._CAND_CACHE["at"] = None
        now = datetime.utcnow()
        account(db, "waiting", synced=now - timedelta(hours=2))
        db.add(Job(id="j1", user_id="u1", status="completed", has_export=True,
                   config_json="{}", dialogue_json="[]", api_keys_json="{}",
                   images_dir="x", output_dir="x", published_at=None, archived=False,
                   created_at=now))
        db.commit()
        acc, reason = autosync.pick_account(db, interval_hours=24, now=now)
        assert acc is not None and reason == "waiting-jobs"

    def test_an_OLD_unpublished_export_is_not_a_reason(self, db):
        # Measured on the live platform: 252 jobs are exported-and-unpublished,
        # but only 2 within 14 days and ZERO within 3. Unscoped, this gate would
        # never close and would be no gate at all.
        from models import Job
        autosync._CAND_CACHE["at"] = None
        now = datetime.utcnow()
        account(db, "waiting", synced=now - timedelta(hours=2))
        db.add(Job(id="j-old", user_id="u1", status="completed", has_export=True,
                   config_json="{}", dialogue_json="[]", api_keys_json="{}",
                   images_dir="x", output_dir="x", published_at=None, archived=False,
                   created_at=now - timedelta(days=40)))
        db.commit()
        acc, reason = autosync.pick_account(db, interval_hours=24, now=now)
        assert acc is None, f"a 40-day-old abandoned export triggered a sync ({reason})"

    def test_a_published_job_is_not_a_reason(self, db):
        from models import Job
        autosync._CAND_CACHE["at"] = None
        now = datetime.utcnow()
        account(db, "waiting", synced=now - timedelta(hours=2))
        db.add(Job(id="j-done", user_id="u1", status="completed", has_export=True,
                   config_json="{}", dialogue_json="[]", api_keys_json="{}",
                   images_dir="x", output_dir="x", published_at=now, archived=False,
                   created_at=now))
        db.commit()
        assert autosync.pick_account(db, interval_hours=24, now=now)[0] is None

    def test_the_backstop_still_catches_what_no_signal_reports(self, db):
        # A reel posted by hand from a phone tells us nothing. The clock's
        # honest job is to catch exactly that — and only that.
        autosync._CAND_CACHE["at"] = None
        now = datetime.utcnow()
        account(db, "quiet", synced=now - timedelta(hours=30))
        acc, reason = autosync.pick_account(db, interval_hours=24, now=now)
        assert acc is not None and reason == "backstop"

    def test_no_accounts_at_all_is_not_an_error(self, db):
        assert autosync.pick_account(db, interval_hours=6)[0] is None

    def test_a_failing_account_backs_off_and_does_not_spin(self, db):
        # The failure clock is last_sync_attempt_at, NOT last_synced_at: a sync
        # that raises never reaches the success bump, so without a separate
        # attempt clock a broken account is retried on every single tick.
        now = datetime.utcnow()
        account(db, "broken", synced=None, attempted=now - timedelta(minutes=5))
        assert autosync.pick_account(db, interval_hours=6, retry_minutes=30,
                                     now=now)[0] is None
        assert autosync.pick_account(db, interval_hours=6, retry_minutes=1,
                                     now=now)[0] is not None

    def test_backoff_gates_a_NUDGE_too(self, db):
        # A broken handle must not spin on a request it cannot satisfy.
        now = datetime.utcnow()
        a = account(db, "broken", synced=None, attempted=now - timedelta(minutes=2))
        autosync.request_sync(db, a, "published")
        db.commit()
        assert autosync.pick_account(db, retry_minutes=30, now=now)[0] is None

    def test_a_recent_attempt_does_not_hold_back_a_healthy_stale_account(self, db):
        # Retry backoff must gate only the account that just failed, never the
        # queue: one broken account cannot be allowed to starve the others.
        now = datetime.utcnow()
        account(db, "broken", synced=None, attempted=now - timedelta(minutes=1))
        stale = account(db, "stale", synced=now - timedelta(days=3),
                        attempted=now - timedelta(days=3))
        acc, _ = autosync.pick_account(db, interval_hours=6, retry_minutes=30,
                                       now=now)
        assert acc.id == stale.id


class TestSyncAccountOnce:
    def test_a_failed_sync_records_the_reason_and_leaves_last_synced_alone(self, db):
        a = account(db, "broken")

        def boom(acc, session):
            raise RuntimeError("HikerAPI 402 out of credit")

        res = autosync.sync_account_once(a, db, fetcher=boom)
        db.refresh(a)
        assert a.last_synced_at is None
        assert a.last_sync_attempt_at is not None
        assert "402" in a.last_sync_error
        assert res["ok"] is False

    def test_a_successful_sync_clears_the_error_and_stamps_both_clocks(self, db):
        a = account(db, "ok", error="old failure")
        res = autosync.sync_account_once(
            a, db, fetcher=lambda acc, s: {"added": 2, "total": 9})
        db.refresh(a)
        assert a.last_synced_at is not None
        assert a.last_sync_attempt_at is not None
        assert a.last_sync_error is None
        assert res["added"] == 2 and res["ok"] is True

    def test_a_long_error_is_truncated_rather_than_blowing_the_column(self, db):
        a = account(db, "verbose")

        def boom(acc, session):
            raise RuntimeError("x" * 5000)

        autosync.sync_account_once(a, db, fetcher=boom)
        db.refresh(a)
        assert len(a.last_sync_error) <= 500


class TestWorkerPass:
    def test_the_pass_syncs_one_account_per_tick(self, db):
        # One per tick bounds the HikerAPI spend and keeps a slow account from
        # stalling the whole worker loop.
        now = datetime.utcnow()
        account(db, "a", synced=now - timedelta(days=3))
        account(db, "b", synced=now - timedelta(days=4))
        seen = []
        n = autosync.process_instagram_autosync(
            db, fetcher=lambda acc, s: seen.append(acc.handle) or {"added": 0})
        assert n == 1
        assert len(seen) == 1

    def test_the_pass_is_a_no_op_when_nothing_is_stale(self, db):
        account(db, "fresh", synced=datetime.utcnow())
        assert autosync.process_instagram_autosync(db, fetcher=None) == 0

    def test_a_poisoned_session_still_records_the_attempt(self, db):
        """The existing failure test raises on a CLEAN session — the one shape
        that already worked.

        When the failure comes from the database the session needs a rollback
        first, and the error-path commit would otherwise raise
        PendingRollbackError out of a function whose docstring promises it never
        raises, leaving both clocks unwritten so nothing records why the account
        went stale.
        """
        a = account(db, "poisoned")

        def poison(acc, session):
            # Put the session into a state where the next commit fails, the way
            # a mid-sync integrity error or dropped connection does.
            session.execute(text("SELECT * FROM no_such_table"))

        res = autosync.sync_account_once(a, db, fetcher=poison)
        assert res["ok"] is False
        db.rollback()
        db.refresh(a)
        assert a.last_sync_attempt_at is not None, "the attempt clock was lost"
        assert a.last_synced_at is None
        assert a.last_sync_error

    def test_a_failing_success_commit_is_recorded_not_raised(self, db, monkeypatch):
        # autoflush=False means every INSERT of the walk lands at the success
        # commit. A clash with the manual Sync button used to escape from there
        # and discard the whole paid walk with both clocks unwritten.
        a = account(db, "clash")
        real_commit = db.commit
        state = {"n": 0}

        def flaky_commit():
            state["n"] += 1
            if state["n"] == 1:
                raise RuntimeError("UNIQUE constraint failed")
            return real_commit()

        monkeypatch.setattr(db, "commit", flaky_commit)
        res = autosync.sync_account_once(
            a, db, fetcher=lambda acc, s: {"added": 3, "total": 9})
        assert res["ok"] is False
        monkeypatch.undo()
        db.refresh(a)
        assert a.last_synced_at is None, "a failed commit must not look synced"
        assert a.last_sync_error

    def test_an_empty_answer_for_a_stocked_account_is_an_outage(self, db):
        """fetch_recent_clips swallows HikerAPIError and returns [].

        Without this guard a total outage looked like "no new reels": the sync
        clock advanced, the error was CLEARED, the retry never engaged, and the
        account went quiet for six hours showing green — the exact blindness
        this module exists to end, asserted as success.
        """
        from models import InstagramVideo
        a = account(db, "stocked")
        db.add(InstagramVideo(account_id=a.id, shortcode="DcAAAAAAAAA",
                              url="https://instagram.com/reel/DcAAAAAAAAA/"))
        db.commit()

        # Drive the REAL _fetch_and_store. Passing a fetcher that re-implements
        # the guard would have left the guard itself untested -- deleting it
        # from the module would not have failed anything.
        autosync_mod = autosync

        def no_clips(*a_, **kw):
            return []

        import instagram_client
        orig_fetch = instagram_client.fetch_recent_clips
        orig_resolve = instagram_client.resolve_user_id
        instagram_client.fetch_recent_clips = no_clips
        instagram_client.resolve_user_id = lambda *a_, **kw: "1"
        try:
            import encryption
            orig_dec = encryption.decrypt
            encryption.decrypt = lambda *a_, **kw: "key"
            try:
                res = autosync_mod.sync_account_once(a, db)
            finally:
                encryption.decrypt = orig_dec
        finally:
            instagram_client.fetch_recent_clips = orig_fetch
            instagram_client.resolve_user_id = orig_resolve

        assert res["ok"] is False, "an outage was recorded as a successful sync"
        db.refresh(a)
        assert a.last_synced_at is None
        assert "no clips" in a.last_sync_error

    def test_an_empty_answer_for_a_brand_new_account_is_fine(self, db):
        # A genuinely empty account must still sync cleanly through the REAL
        # code path, or a new handle could never get off the ground.
        a = account(db, "brand-new")
        import instagram_client, encryption
        orig_fetch = instagram_client.fetch_recent_clips
        orig_resolve = instagram_client.resolve_user_id
        orig_dec = encryption.decrypt
        instagram_client.fetch_recent_clips = lambda *a_, **kw: []
        instagram_client.resolve_user_id = lambda *a_, **kw: "1"
        encryption.decrypt = lambda *a_, **kw: "key"
        try:
            res = autosync.sync_account_once(a, db)
        finally:
            instagram_client.fetch_recent_clips = orig_fetch
            instagram_client.resolve_user_id = orig_resolve
            encryption.decrypt = orig_dec
        assert res["ok"] is True
        db.refresh(a)
        assert a.last_synced_at is not None

    def test_a_raising_fetcher_never_escapes_the_pass(self, db):
        # This runs inside the worker's main loop. An exception that got out
        # would take down job processing with it.
        account(db, "broken", synced=datetime.utcnow() - timedelta(days=9))

        def boom(acc, session):
            raise RuntimeError("network gone")

        assert autosync.process_instagram_autosync(db, fetcher=boom) == 1
