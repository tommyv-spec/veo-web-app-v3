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
    def test_picks_the_stalest_account_first(self, db):
        now = datetime.utcnow()
        account(db, "recent", synced=now - timedelta(hours=7))
        old = account(db, "ancient", synced=now - timedelta(days=56))
        assert autosync.pick_account(db, interval_hours=6, now=now).id == old.id

    def test_an_account_synced_inside_the_interval_is_skipped(self, db):
        now = datetime.utcnow()
        account(db, "fresh", synced=now - timedelta(hours=1))
        assert autosync.pick_account(db, interval_hours=6, now=now) is None

    def test_a_never_synced_account_is_picked(self, db):
        # NULL last_synced_at must sort as infinitely stale, not as "no opinion".
        now = datetime.utcnow()
        a = account(db, "new")
        assert autosync.pick_account(db, interval_hours=6, now=now).id == a.id

    def test_no_accounts_at_all_is_not_an_error(self, db):
        assert autosync.pick_account(db, interval_hours=6) is None

    def test_a_failing_account_backs_off_and_does_not_spin(self, db):
        # The failure clock is last_sync_attempt_at, NOT last_synced_at: a sync
        # that raises never reaches the success bump, so without a separate
        # attempt clock a broken account is retried on every single tick.
        now = datetime.utcnow()
        account(db, "broken", synced=None, attempted=now - timedelta(minutes=5))
        assert autosync.pick_account(db, interval_hours=6, retry_minutes=30,
                                     now=now) is None
        assert autosync.pick_account(db, interval_hours=6, retry_minutes=1,
                                     now=now) is not None

    def test_a_recent_attempt_does_not_hold_back_a_healthy_stale_account(self, db):
        # Retry backoff must gate only the account that just failed, never the
        # queue: one broken account cannot be allowed to starve the others.
        now = datetime.utcnow()
        account(db, "broken", synced=None, attempted=now - timedelta(minutes=1))
        stale = account(db, "stale", synced=now - timedelta(days=3),
                        attempted=now - timedelta(days=3))
        assert autosync.pick_account(db, interval_hours=6, retry_minutes=30,
                                     now=now).id == stale.id


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
        monkey = {}

        def no_clips(acc, session):
            # Mirror _fetch_and_store's guard without touching HikerAPI.
            stored = session.query(InstagramVideo).filter_by(account_id=acc.id).count()
            if stored:
                raise RuntimeError("HikerAPI returned no clips while reels are stored")
            return {"added": 0, "total": 0}

        res = autosync.sync_account_once(a, db, fetcher=no_clips)
        assert res["ok"] is False
        db.refresh(a)
        assert a.last_synced_at is None
        assert "no clips" in a.last_sync_error

    def test_an_empty_answer_for_a_brand_new_account_is_fine(self, db):
        # A genuinely empty account must still sync cleanly, or a new handle
        # could never get off the ground.
        a = account(db, "brand-new")
        res = autosync.sync_account_once(
            a, db, fetcher=lambda acc, s: {"added": 0, "total": 0})
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
